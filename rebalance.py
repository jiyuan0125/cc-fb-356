#!/usr/bin/env python3
"""Shrink 9 shards to 6 according to plan.json (bucket -> target shard).

Migration rule at the cutoff instant:
  * ingest_ts <= cutoff            -> moved by this migration
  * ingest_ts >  cutoff, event_ts <  cutoff  -> late event, supplemented afterwards
                                     and still counted in this bucket's migration row
  * ingest_ts >  cutoff, event_ts >= cutoff  -> not part of this migration

Outputs:
  new_shards/shard-01..06.jsonl   migrated rows, one file per target shard
  migration.csv                   bucket,from,to,cnt per bucket + consistency totals
  routing_v2.json                  bucket -> target shard
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SHARD_FILE_RE = re.compile(r"shard-(\d+)\.jsonl$")


def parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp, tolerating a trailing Z."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    ts = datetime.fromisoformat(text)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def shard_sort_key(path: Path) -> tuple[int, str]:
    m = SHARD_FILE_RE.search(path.name)
    return (int(m.group(1)), path.name) if m else (sys.maxsize, path.name)


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{lineno}: invalid JSON: {exc}") from exc
            rows.append(rec)
    return rows


def main() -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shards", type=Path, default=here / "shards")
    ap.add_argument("--new-shards", type=Path, default=here / "new_shards")
    ap.add_argument("--routing", type=Path, default=here / "routing.json")
    ap.add_argument("--plan", type=Path, default=here / "plan.json")
    ap.add_argument("--migration-csv", type=Path, default=here / "migration.csv")
    ap.add_argument("--routing-v2", type=Path, default=here / "routing_v2.json")
    args = ap.parse_args()

    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    routing = json.loads(args.routing.read_text(encoding="utf-8"))
    targets: dict[str, str] = plan["targets"]
    cutoff = parse_ts(plan["cutoff_ts"])

    missing_targets = set(routing) - set(targets)
    if missing_targets:
        raise SystemExit(f"buckets without a plan target: {sorted(missing_targets)}")
    target_shards = sorted(set(targets.values()))

    shard_files = sorted(args.shards.glob("*.jsonl"), key=shard_sort_key)
    if not shard_files:
        raise SystemExit(f"no shard files under {args.shards}")

    # --- classify rows and route them to their target shard -----------------
    included: dict[str, list[dict]] = {name: [] for name in target_shards}
    per_bucket: dict[str, int] = {}
    stats = {"normal": 0, "late": 0, "excluded": 0}
    seen_ids: set[str] = set()

    for path in shard_files:
        source_shard = path.name[: -len(".jsonl")]
        for rec in read_jsonl(path):
            bucket = rec["bucket"]
            if bucket not in targets:
                raise SystemExit(f"{path}: bucket {bucket!r} has no plan target")
            if routing.get(bucket) != source_shard:
                raise SystemExit(
                    f"{path}: bucket {bucket!r} stored in {source_shard!r} but "
                    f"routing.json says {routing.get(bucket)!r}"
                )

            event_id = rec["event_id"]
            if event_id in seen_ids:
                raise SystemExit(f"duplicate event_id {event_id!r}")
            seen_ids.add(event_id)

            ingest_ts = parse_ts(rec["ingest_ts"])
            event_ts = parse_ts(rec["event_ts"])

            if ingest_ts <= cutoff:
                stats["normal"] += 1
            elif event_ts < cutoff:
                stats["late"] += 1          # late arrival, supplemented after sealing
            else:
                stats["excluded"] += 1      # belongs to a later generation
                continue

            target = targets[bucket]
            if target not in included:
                raise SystemExit(f"plan target {target!r} is not one of {target_shards}")
            included[target].append(rec)
            per_bucket[bucket] = per_bucket.get(bucket, 0) + 1

    # --- write the six new shards -------------------------------------------
    args.new_shards.mkdir(parents=True, exist_ok=True)
    stale = sorted(
        p.name
        for p in args.new_shards.glob("*.jsonl")
        if p.name not in {f"{name}.jsonl" for name in target_shards}
    )
    if stale:
        print(f"warning: stray files left untouched in {args.new_shards}: {stale}", file=sys.stderr)

    written = 0
    for name in target_shards:
        out = args.new_shards / f"{name}.jsonl"
        tmp = out.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for rec in included[name]:
                fh.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
                fh.write("\n")
                written += 1
        tmp.replace(out)

    # --- migration.csv -------------------------------------------------------
    new_total = 0
    for name in target_shards:
        new_total += sum(1 for _ in (args.new_shards / f"{name}.jsonl").open(encoding="utf-8"))

    old_total = written                       # events taken in by this migration
    csv_total = sum(per_bucket.values())
    diff = old_total - new_total

    args.migration_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.migration_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(["bucket", "from", "to", "cnt"])
        for bucket in sorted(targets):
            writer.writerow([bucket, routing[bucket], targets[bucket], per_bucket.get(bucket, 0)])
        writer.writerow(["consistency", old_total, new_total, csv_total, diff])

    # --- routing_v2.json -----------------------------------------------------
    args.routing_v2.write_text(
        json.dumps(dict(sorted(targets.items())), ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8",
    )

    print(
        f"cutoff={plan['cutoff_ts']} normal={stats['normal']} late={stats['late']} "
        f"excluded={stats['excluded']} -> migrated={old_total}",
        file=sys.stderr,
    )
    print(
        f"old_total={old_total} new_total={new_total} csv_total={csv_total} diff={diff}",
        file=sys.stderr,
    )

    if diff != 0 or old_total != csv_total:
        print("consistency check FAILED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
