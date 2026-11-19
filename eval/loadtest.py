#!/usr/bin/env python3
"""Evaluation harness for the job scheduler. Standard library only.

Each run:
  1. submits N "sleep" jobs with a HIGH/NORMAL/LOW priority mix,
  2. optionally SIGKILLs random worker containers during the run and restarts them,
  3. polls until every job is SUCCEEDED or DEAD, or the timeout passes,
  4. writes results.json and summary.md to --out.

Examples (run from the job-scheduler directory, with `docker compose up -d --scale worker=3`):
  python3 eval/loadtest.py --jobs 2000 --kill 0 --out eval/results/baseline
  python3 eval/loadtest.py --jobs 2000 --kill 3 --out eval/results/kill-3
"""

import argparse
import json
import random
import re
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

TERMINAL = {"SUCCEEDED", "DEAD"}
PRIORITIES = ["HIGH", "NORMAL", "LOW"]


# ---------- HTTP and timestamps ----------

def http_json(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def parse_ts(value):
    """Parse Java Instant strings, which may carry more fractional digits than Python accepts."""
    if not value:
        return None
    value = re.sub(r"(\.\d{6})\d+", r"\1", value).replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def latency_ms(view):
    return (parse_ts(view["completedAt"]) - parse_ts(view["createdAt"])).total_seconds() * 1000


# ---------- docker control ----------

def compose(project_dir, *args):
    return subprocess.run(["docker", "compose", *args], cwd=project_dir,
                          capture_output=True, text=True)


def worker_container_ids(project_dir):
    return compose(project_dir, "ps", "-q", "worker").stdout.split()


def kill_workers(project_dir, count, gap_s, rng):
    events = []
    for _ in range(count):
        ids = worker_container_ids(project_dir)
        if not ids:
            break
        victim = rng.choice(ids)
        killed_at = datetime.now(timezone.utc)
        subprocess.run(["docker", "kill", "--signal", "SIGKILL", victim],
                       capture_output=True, check=True)
        events.append({"container": victim[:12], "killed_at": killed_at.isoformat()})
        time.sleep(gap_s)
    return events


def restore_workers(project_dir, replicas):
    compose(project_dir, "up", "-d", "--no-recreate", "--scale", f"worker={replicas}")


# ---------- workload ----------

def submit_all(api, run_id, n, weights, sleep_ms, seed, threads=16):
    rng = random.Random(seed)
    plan = [{"priority": rng.choices(PRIORITIES, weights=weights)[0], "key": f"{run_id}-{i}"}
            for i in range(n)]

    def submit(job):
        body = {
            "type": "sleep",
            "payload": {"durationMs": sleep_ms, "failureRate": 0.0},
            "priority": job["priority"],
            "maxAttempts": 5,
            "idempotencyKey": job["key"],
        }
        t0 = time.time()
        view = http_json("POST", f"{api}/api/jobs", body)
        return {"id": view["id"], "priority": job["priority"],
                "submit_ms": (time.time() - t0) * 1000}

    with ThreadPoolExecutor(max_workers=threads) as pool:
        return list(pool.map(submit, plan))


def fetch_job(api, job_id):
    try:
        return http_json("GET", f"{api}/api/jobs/{job_id}")
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return None


def wait_for_terminal(api, job_ids, timeout_s):
    deadline = time.time() + timeout_s
    final, pending = {}, set(job_ids)
    with ThreadPoolExecutor(max_workers=16) as pool:
        while pending and time.time() < deadline:
            for view in pool.map(lambda i: fetch_job(api, i), list(pending)):
                if view and view["status"] in TERMINAL:
                    final[view["id"]] = view
                    pending.discard(view["id"])
            if pending:
                time.sleep(1.0)
    return final, pending


# ---------- analysis ----------

def analyze(submitted, final, pending, kill_events, config, wall_s):
    succeeded = [v for v in final.values() if v["status"] == "SUCCEEDED"]
    dead = [v for v in final.values() if v["status"] == "DEAD"]
    recovered = [v for v in succeeded if v["attempts"] > 1]
    clean = [v for v in succeeded if v["attempts"] == 1]

    per_priority = {}
    for p in PRIORITIES:
        vals = [latency_ms(v) for v in succeeded if v["priority"] == p]
        per_priority[p] = {
            "count": len(vals),
            "p50_ms": pct(vals, 50), "p95_ms": pct(vals, 95), "p99_ms": pct(vals, 99),
        }

    all_lat = [latency_ms(v) for v in succeeded]
    rec_lat = [latency_ms(v) for v in recovered]
    clean_lat = [latency_ms(v) for v in clean]

    throughput = None
    if succeeded:
        start = min(parse_ts(v["createdAt"]) for v in succeeded)
        end = max(parse_ts(v["completedAt"]) for v in succeeded)
        span = (end - start).total_seconds()
        throughput = len(succeeded) / span if span > 0 else None

    return {
        "config": config,
        "wall_clock_s": round(wall_s, 2),
        "counts": {
            "submitted": len(submitted),
            "succeeded": len(succeeded),
            "dead": len(dead),
            "unfinished_at_timeout": len(pending),
            "recovered_after_crash": len(recovered),
            "max_attempts_seen": max((v["attempts"] for v in final.values()), default=0),
        },
        "throughput_jobs_per_s": throughput,
        "submit_latency_ms_p95": pct([s["submit_ms"] for s in submitted], 95),
        "end_to_end_latency_ms": {"p50": pct(all_lat, 50), "p95": pct(all_lat, 95), "p99": pct(all_lat, 99)},
        "latency_by_priority_ms": per_priority,
        # Proxy for recovery overhead: how much slower recovered jobs were than clean ones.
        "recovery_overhead_ms": {
            "recovered_p50": pct(rec_lat, 50),
            "recovered_p95": pct(rec_lat, 95),
            "clean_p50": pct(clean_lat, 50),
            "extra_p50": (pct(rec_lat, 50) - pct(clean_lat, 50)) if rec_lat and clean_lat else None,
        },
        "kill_events": kill_events,
    }


def fmt(x, unit=""):
    if x is None:
        return "n/a"
    return f"{x:,.1f}{unit}" if isinstance(x, float) else f"{x:,}{unit}"


def render_markdown(r):
    c, cfg = r["counts"], r["config"]
    lines = [
        "# Evaluation run",
        "",
        f"Config: {json.dumps(cfg)}",
        "",
        "## Correctness",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Submitted | {fmt(c['submitted'])} |",
        f"| Succeeded | {fmt(c['succeeded'])} |",
        f"| Dead (retries exhausted) | {fmt(c['dead'])} |",
        f"| Unfinished at timeout (lost or stuck) | {fmt(c['unfinished_at_timeout'])} |",
        f"| Recovered after a worker crash | {fmt(c['recovered_after_crash'])} |",
        f"| Max attempts seen | {fmt(c['max_attempts_seen'])} |",
        f"| Worker kills performed | {fmt(len(r['kill_events']))} |",
        "",
        "## Performance",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Throughput | {fmt(r['throughput_jobs_per_s'], ' jobs/s')} |",
        f"| End-to-end latency p50 / p95 / p99 | {fmt(r['end_to_end_latency_ms']['p50'], ' ms')} / "
        f"{fmt(r['end_to_end_latency_ms']['p95'], ' ms')} / {fmt(r['end_to_end_latency_ms']['p99'], ' ms')} |",
        f"| Submit latency p95 | {fmt(r['submit_latency_ms_p95'], ' ms')} |",
        f"| Wall clock | {fmt(r['wall_clock_s'], ' s')} |",
        "",
        "## Latency by priority (end-to-end, ms)",
        "",
        "| Priority | Count | p50 | p95 | p99 |",
        "|---|---|---|---|---|",
    ]
    for p, s in r["latency_by_priority_ms"].items():
        lines.append(f"| {p} | {fmt(s['count'])} | {fmt(s['p50_ms'])} | {fmt(s['p95_ms'])} | {fmt(s['p99_ms'])} |")

    ro = r["recovery_overhead_ms"]
    lines += [
        "",
        "## Recovery overhead",
        "",
        "Recovered jobs compared with jobs that ran on the first attempt. This is a proxy: "
        "it includes lease detection, the retry dispatch, and re-execution.",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Recovered job latency p50 / p95 | {fmt(ro['recovered_p50'], ' ms')} / {fmt(ro['recovered_p95'], ' ms')} |",
        f"| Clean job latency p50 | {fmt(ro['clean_p50'], ' ms')} |",
        f"| Extra latency for recovered jobs (p50) | {fmt(ro['extra_p50'], ' ms')} |",
        "",
    ]
    return "\n".join(lines)


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:8080")
    ap.add_argument("--jobs", type=int, default=2000)
    ap.add_argument("--mix", default="0.2,0.6,0.2", help="weights for HIGH,NORMAL,LOW")
    ap.add_argument("--sleep-ms", type=int, default=200, help="simulated work per job")
    ap.add_argument("--kill", type=int, default=0, help="number of worker containers to SIGKILL")
    ap.add_argument("--kill-after", type=float, default=5.0, help="seconds after start before first kill")
    ap.add_argument("--kill-gap", type=float, default=10.0, help="seconds between kills")
    ap.add_argument("--replicas", type=int, default=3, help="worker replicas to restore after kills")
    ap.add_argument("--timeout", type=float, default=300.0, help="max seconds to wait for completion")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--project-dir", default=str(Path(__file__).resolve().parents[1]))
    ap.add_argument("--out", default="eval/results/run")
    args = ap.parse_args()

    weights = [float(x) for x in args.mix.split(",")]
    if len(weights) != 3:
        raise SystemExit("--mix needs three comma-separated weights: HIGH,NORMAL,LOW")

    run_id = f"eval-{int(time.time())}"
    rng = random.Random(args.seed)
    kill_events = []
    killer = None
    if args.kill > 0:
        def kill_thread():
            time.sleep(args.kill_after)
            kill_events.extend(kill_workers(args.project_dir, args.kill, args.kill_gap, rng))
            restore_workers(args.project_dir, args.replicas)
        killer = threading.Thread(target=kill_thread, daemon=True)
        killer.start()

    t0 = time.time()
    submitted = submit_all(args.api, run_id, args.jobs, weights, args.sleep_ms, args.seed)
    print(f"submitted {len(submitted)} jobs; waiting for completion...", flush=True)
    final, pending = wait_for_terminal(args.api, [s["id"] for s in submitted], args.timeout)
    if killer is not None:
        killer.join(timeout=args.kill * args.kill_gap + 60)
    wall = time.time() - t0

    config = {
        "jobs": args.jobs, "priority_mix_high_normal_low": weights, "sleep_ms": args.sleep_ms,
        "worker_kills": args.kill, "kill_after_s": args.kill_after, "kill_gap_s": args.kill_gap,
        "worker_replicas": args.replicas, "timeout_s": args.timeout, "seed": args.seed,
    }
    result = analyze(submitted, final, pending, kill_events, config, wall)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(json.dumps(result, indent=2, default=str))
    report = render_markdown(result)
    (out / "summary.md").write_text(report)
    print(report)
    print(f"\nWrote {out / 'results.json'} and {out / 'summary.md'}")


if __name__ == "__main__":
    main()
