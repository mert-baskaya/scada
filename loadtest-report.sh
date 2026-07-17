#!/usr/bin/env bash
# Samples produce rate, Flink throughput, consumer lag and checkpoint health
# during a load test. Usage: ./loadtest-report.sh [duration_seconds] [interval_seconds]
# Works with all pipeline modes: sums lag over every flink-scada-* consumer group
# (split mode uses one group per event-type topic) and measures Flink consumption
# as committed-offset growth instead of vertex metrics, which double-count fan-out.
set -euo pipefail

DURATION="${1:-300}"
INTERVAL="${2:-20}"
FLINK_URL="http://localhost:8095"
KAFKA_CONTAINER="scada-kafka"
LOADGEN_CONTAINER="scada-load-generator"

py() { python3 -c "$1" 2>/dev/null || echo "n/a"; }

flink_job_id() {
  curl -sf "$FLINK_URL/jobs/overview" | py 'import sys,json
jobs=[(j["jid"],j["name"]) for j in json.load(sys.stdin)["jobs"] if j["state"]=="RUNNING"]
print("%s %s" % jobs[0] if jobs else "")'
}

consumer_groups() {
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null | grep '^flink-scada' || true
}

# prints "<total_committed> <total_lag>" summed across all flink-scada-* groups
offsets_snapshot() {
  local args=()
  local g
  for g in $(consumer_groups); do args+=(--group "$g"); done
  if [[ ${#args[@]} -eq 0 ]]; then echo "n/a n/a"; return; fi
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe "${args[@]}" 2>/dev/null \
    | awk '$4 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/ {c+=$4; l+=$6; n++}
           END {if (n) print c, l; else print "n/a", "n/a"}'
}

produce_rate() {
  docker logs --tail 50 "$LOADGEN_CONTAINER" 2>/dev/null \
    | grep -o 'rate=[0-9,.]*' | tail -1 | cut -d= -f2 | tr -d , || echo "n/a"
}

read -r JOB_ID JOB_NAME <<< "$(flink_job_id)"
if [[ -z "$JOB_ID" ]]; then
  echo "ERROR: no RUNNING Flink job found at $FLINK_URL" >&2
  exit 1
fi
echo "Flink job: $JOB_ID ($JOB_NAME)"
echo "Consumer groups: $(consumer_groups | tr '\n' ' ')"
echo "Sampling every ${INTERVAL}s for ${DURATION}s..."
echo
printf "%-10s %15s %15s %15s\n" "elapsed" "produce ev/s" "flink ev/s" "consumer lag"

START=$(date +%s)
FIRST_LAG=""; LAST_LAG=""; FIRST_T=""; LAST_T=""
FIRST_COMMIT=""; LAST_COMMIT=""
PREV_COMMIT=""; PREV_T=""
SUM_PRODUCE=0; NP=0

while true; do
  NOW=$(date +%s); ELAPSED=$((NOW - START))
  [[ $ELAPSED -gt $DURATION ]] && break
  P="$(produce_rate)"
  read -r COMMIT LAG <<< "$(offsets_snapshot)"
  F="n/a"
  if [[ "$COMMIT" =~ ^[0-9]+$ && "$PREV_COMMIT" =~ ^[0-9]+$ && $NOW -gt $PREV_T ]]; then
    F=$(( (COMMIT - PREV_COMMIT) / (NOW - PREV_T) ))
  fi
  printf "%-10s %15s %15s %15s\n" "${ELAPSED}s" "${P:-n/a}" "$F" "${LAG:-n/a}"
  if [[ "$LAG" =~ ^[0-9]+$ ]]; then
    [[ -z "$FIRST_LAG" ]] && { FIRST_LAG=$LAG; FIRST_T=$NOW; }
    LAST_LAG=$LAG; LAST_T=$NOW
  fi
  if [[ "$COMMIT" =~ ^[0-9]+$ ]]; then
    [[ -z "$FIRST_COMMIT" ]] && FIRST_COMMIT=$COMMIT
    LAST_COMMIT=$COMMIT
    PREV_COMMIT=$COMMIT; PREV_T=$NOW
  fi
  [[ "$P" =~ ^[0-9.]+$ ]] && { SUM_PRODUCE=$(echo "$SUM_PRODUCE + $P" | bc); NP=$((NP+1)); }
  sleep "$INTERVAL"
done

echo
echo "=== Summary ==="
[[ $NP -gt 0 ]] && echo "avg produce rate:    $(echo "$SUM_PRODUCE / $NP" | bc) ev/s"
if [[ -n "$FIRST_COMMIT" && "$LAST_T" -gt "$FIRST_T" ]]; then
  echo "avg flink consumed:  $(( (LAST_COMMIT - FIRST_COMMIT) / (LAST_T - FIRST_T) )) ev/s (committed-offset growth)"
fi
if [[ -n "$FIRST_LAG" && "$LAST_T" -gt "$FIRST_T" ]]; then
  GROWTH=$(( (LAST_LAG - FIRST_LAG) / (LAST_T - FIRST_T) ))
  echo "consumer lag:        $FIRST_LAG -> $LAST_LAG (growth ${GROWTH} ev/s)"
fi

echo
echo "=== Busiest operators (busyTimeMsPerSecond, avg per subtask) ==="
curl -sf "$FLINK_URL/jobs/$JOB_ID" | py 'import sys,json,urllib.request
job=json.load(sys.stdin)
rows=[]
for v in job["vertices"]:
    url="'"$FLINK_URL/jobs/$JOB_ID"'/vertices/" + v["id"] + "/subtasks/metrics?get=busyTimeMsPerSecond&agg=avg"
    try:
        m=json.load(urllib.request.urlopen(url))
        if m: rows.append((m[0].get("avg",0), v["name"][:70]))
    except Exception: pass
for busy,name in sorted(rows,reverse=True):
    print("  %7.0f ms/s  %s" % (busy, name))'

echo
echo "=== Checkpoints ==="
curl -sf "$FLINK_URL/jobs/$JOB_ID/checkpoints" | py 'import sys,json
c=json.load(sys.stdin)
counts=c["counts"]; latest=c.get("latest",{}).get("completed") or {}
print("  completed=%s failed=%s in_progress=%s" % (counts["completed"], counts["failed"], counts["in_progress"]))
if latest:
    print("  latest: duration=%sms size=%s bytes" % (latest["end_to_end_duration"], latest["state_size"]))'
