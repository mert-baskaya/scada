#!/usr/bin/env bash
# Samples produce rate, Flink throughput, consumer lag and checkpoint health
# during a load test. Usage: ./loadtest-report.sh [duration_seconds] [interval_seconds]
set -euo pipefail

DURATION="${1:-300}"
INTERVAL="${2:-15}"
FLINK_URL="http://localhost:8095"
GROUP="flink-scada-processor"
KAFKA_CONTAINER="scada-kafka"
LOADGEN_CONTAINER="scada-load-generator"

py() { python3 -c "$1" 2>/dev/null || echo "n/a"; }

flink_job_id() {
  curl -sf "$FLINK_URL/jobs" | py 'import sys,json
jobs=[j["id"] for j in json.load(sys.stdin)["jobs"] if j["status"]=="RUNNING"]
print(jobs[0] if jobs else "")'
}

source_vertex_id() {
  curl -sf "$FLINK_URL/jobs/$1" | py "import sys,json
for v in json.load(sys.stdin)['vertices']:
    if 'Kafka Source' in v['name']:
        print(v['id']); break"
}

flink_source_rate() {
  curl -sf "$FLINK_URL/jobs/$1/vertices/$2/subtasks/metrics?get=numRecordsOutPerSecond&agg=sum" \
    | py 'import sys,json
m=json.load(sys.stdin)
print(int(m[0]["sum"]) if m else "n/a")'
}

produce_rate() {
  docker logs --tail 50 "$LOADGEN_CONTAINER" 2>/dev/null \
    | grep -o 'rate=[0-9,.]*' | tail -1 | cut -d= -f2 | tr -d , || echo "n/a"
}

total_lag() {
  # group only exists after Flink's first successful checkpoint commits offsets
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ {s+=$6; n++} END {if (n) print s; else print "n/a"}'
}

JOB_ID="$(flink_job_id)"
if [[ -z "$JOB_ID" ]]; then
  echo "ERROR: no RUNNING Flink job found at $FLINK_URL" >&2
  exit 1
fi
SRC_ID="$(source_vertex_id "$JOB_ID")"
echo "Flink job: $JOB_ID (source vertex: $SRC_ID)"
echo "Sampling every ${INTERVAL}s for ${DURATION}s..."
echo
printf "%-10s %15s %15s %15s\n" "elapsed" "produce ev/s" "flink ev/s" "consumer lag"

START=$(date +%s)
FIRST_LAG=""; LAST_LAG=""; FIRST_T=""; LAST_T=""
SUM_PRODUCE=0; SUM_FLINK=0; N=0

while true; do
  NOW=$(date +%s); ELAPSED=$((NOW - START))
  [[ $ELAPSED -gt $DURATION ]] && break
  P="$(produce_rate)"; F="$(flink_source_rate "$JOB_ID" "$SRC_ID")"; L="$(total_lag)"
  printf "%-10s %15s %15s %15s\n" "${ELAPSED}s" "${P:-n/a}" "${F:-n/a}" "${L:-n/a}"
  if [[ "$L" =~ ^[0-9]+$ ]]; then
    [[ -z "$FIRST_LAG" ]] && { FIRST_LAG=$L; FIRST_T=$NOW; }
    LAST_LAG=$L; LAST_T=$NOW
  fi
  [[ "$P" =~ ^[0-9.]+$ ]] && SUM_PRODUCE=$(echo "$SUM_PRODUCE + $P" | bc)
  [[ "$F" =~ ^[0-9.]+$ ]] && { SUM_FLINK=$(echo "$SUM_FLINK + $F" | bc); N=$((N+1)); }
  sleep "$INTERVAL"
done

echo
echo "=== Summary ==="
if [[ $N -gt 0 ]]; then
  echo "avg produce rate:    $(echo "$SUM_PRODUCE / $N" | bc) ev/s"
  echo "avg flink consumed:  $(echo "$SUM_FLINK / $N" | bc) ev/s"
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
