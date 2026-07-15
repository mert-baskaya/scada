#!/usr/bin/env bash
# Infrastructure fault-injection scenarios for the SCADA Flink demo.
# Each scenario injects a fault, then shows recovery evidence from the
# Flink REST API (:8095) and the Spring API (:8096).
set -euo pipefail

FLINK=http://localhost:8095
API=http://localhost:8096
NETWORK=scada_default

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
say()  { printf '  %s\n' "$*"; }

job_id() {
  curl -sf "$FLINK/jobs/overview" 2>/dev/null | python3 -c '
import json,sys
jobs=[j for j in json.load(sys.stdin)["jobs"] if j["state"] not in ("FAILED","CANCELED","FINISHED")]
print(jobs[0]["jid"] if jobs else "")' 2>/dev/null || true
}

job_state() {
  local jid
  jid=$(job_id) || true
  [ -n "${jid:-}" ] && curl -sf "$FLINK/jobs/$jid" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' || echo "UNREACHABLE"
}

checkpoint_summary() {
  local jid
  jid=$(job_id) || true
  if [ -z "${jid:-}" ]; then say "checkpoints: jobmanager unreachable or no job"; return; fi
  curl -sf "$FLINK/jobs/$jid/checkpoints" | python3 -c '
import json,sys
d=json.load(sys.stdin)
c=d["counts"]; latest=d.get("latest") or {}
comp=(latest.get("completed") or {})
rest=(latest.get("restored") or {})
print("  checkpoints: %s completed, %s failed, %s in progress" % (c["completed"], c["failed"], c["in_progress"]))
if comp: print("  latest completed: id=%s (%s bytes state)" % (comp["id"], comp["state_size"]))
if rest: print("  LATEST RESTORE: checkpoint id=%s  <-- state recovered from here" % rest["id"])'
}

taskmanagers() {
  curl -sf "$FLINK/taskmanagers" | python3 -c '
import json,sys
tms=json.load(sys.stdin)["taskmanagers"]
print("  taskmanagers registered: %d" % len(tms))
for t in tms: print("    %s  slots free %s/%s" % (t["id"], t["freeSlots"], t["slotsNumber"]))' \
    || say "taskmanagers: jobmanager unreachable"
}

counts() {
  local aggs alerts
  aggs=$(curl -sf "$API/api/aggregates?limit=1000" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo "?")
  alerts=$(curl -sf "$API/api/alerts?limit=1000" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo "?")
  say "downstream rows: aggregates=$aggs alerts=$alerts"
}

status() {
  bold "== Pipeline status =="
  say "job state: $(job_state)"
  checkpoint_summary
  taskmanagers
  counts
}

# Container name of the TaskManager whose slots are in use (i.e. hosting the job)
busy_taskmanager() {
  local busy_ip
  busy_ip=$(curl -sf "$FLINK/taskmanagers" | python3 -c '
import json,sys
for t in json.load(sys.stdin)["taskmanagers"]:
    if t["freeSlots"] < t["slotsNumber"]:
        print(t["id"].split(":")[0]); break')
  for c in scada-flink-taskmanager scada-flink-taskmanager-2; do
    if docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$c" 2>/dev/null | grep -q "^${busy_ip}$"; then
      echo "$c"; return
    fi
  done
  echo scada-flink-taskmanager
}

wait_for_state() {
  local want=$1 timeout=${2:-120} t=0
  while [ "$t" -lt "$timeout" ]; do
    local s
    s=$(job_state)
    say "t=${t}s job state: $s"
    [ "$s" = "$want" ] && return 0
    sleep 5; t=$((t+5))
  done
  say "timed out after ${timeout}s waiting for $want"
  return 1
}

restore_id() {
  local jid
  jid=$(job_id) || true
  [ -z "${jid:-}" ] && { echo ""; return; }
  curl -sf "$FLINK/jobs/$jid/checkpoints" 2>/dev/null | python3 -c '
import json,sys
rest=(json.load(sys.stdin).get("latest") or {}).get("restored") or {}
print(rest.get("id",""))' || echo ""
}

completed_id() {
  local jid
  jid=$(job_id) || true
  [ -z "${jid:-}" ] && { echo 0; return; }
  curl -sf "$FLINK/jobs/$jid/checkpoints" 2>/dev/null | python3 -c '
import json,sys
comp=(json.load(sys.stdin).get("latest") or {}).get("completed") or {}
print(comp.get("id",0))' || echo 0
}

# Health is proven by a NEW completed checkpoint (source, state, and both
# transactional sinks must all cooperate for one to complete).
wait_for_progress() {
  local prev=$1 timeout=${2:-180} t=0
  while [ "$t" -lt "$timeout" ]; do
    local s c
    s=$(job_state); c=$(completed_id)
    say "t=${t}s job state: $s latest completed checkpoint: ${c:-?} (was $prev)"
    if [ "$s" = "RUNNING" ] && [ "${c:-0}" -gt "$prev" ] 2>/dev/null; then
      return 0
    fi
    sleep 5; t=$((t+5))
  done
  say "timed out after ${timeout}s waiting for checkpoint progress"
  return 1
}

# Recovery is proven by a checkpoint restore, not by job state alone:
# right after a fault the job still reports RUNNING until detection kicks in.
wait_for_recovery() {
  local prev=$1 timeout=${2:-180} t=0
  while [ "$t" -lt "$timeout" ]; do
    local s r
    s=$(job_state); r=$(restore_id)
    say "t=${t}s job state: $s restored-from: ${r:-none}"
    if [ "$s" = "RUNNING" ] && [ -n "$r" ] && [ "$r" != "$prev" ]; then
      return 0
    fi
    sleep 5; t=$((t+5))
  done
  say "timed out after ${timeout}s waiting for checkpoint restore"
  return 1
}

kill_taskmanager() {
  status
  local victim prev
  victim=$(busy_taskmanager)
  prev=$(restore_id)
  bold "== FAULT: SIGKILL $victim (simulates process/node crash) =="
  docker kill "$victim" >/dev/null
  say "killed. JobManager will detect the lost TaskManager, cancel affected"
  say "tasks, and redeploy them onto the standby TaskManager, restoring all"
  say "window + CEP state from the last completed checkpoint."
  bold "== Recovery =="
  wait_for_recovery "$prev" 120
  bold "== Bringing the killed TaskManager back as standby =="
  # restart:always does not cover docker kill (counts as manual stop)
  docker start "$victim" >/dev/null
  sleep 10
  status
  say ""
  say "Note the 'LATEST RESTORE' line above: state came from a checkpoint."
}

kafka_outage() {
  status
  local prev
  prev=$(completed_id)
  bold "== FAULT: stopping Kafka for 30s (broker outage) =="
  docker stop scada-kafka >/dev/null
  say "Kafka down. For a short outage the Kafka clients buffer and retry"
  say "(producer delivery.timeout.ms is 2 min), so tasks usually survive"
  say "without restarting — checkpoints fail or stall, then resume. Only a"
  say "longer outage fails the tasks and engages the exponential-delay"
  say "restart strategy."
  sleep 30
  bold "== Restoring Kafka =="
  docker start scada-kafka >/dev/null
  bold "== Waiting for a fresh completed checkpoint (proves end-to-end health) =="
  wait_for_progress "$prev" 180
  sleep 5
  status
  say ""
  say "Caveat: the simulator produces with acks=0, so telemetry generated"
  say "while Kafka was down was dropped at the producer, not by Flink."
}

restart_jobmanager() {
  status
  bold "== FAULT: restarting JobManager (no HA configured) =="
  docker restart scada-flink-jobmanager >/dev/null
  say "Without high availability (ZooKeeper/K8s), a lost JobManager forgets"
  say "checkpoint metadata: the job is resubmitted FRESH — window/CEP state is"
  say "lost. The Kafka source falls back to committed offsets (committed on"
  say "each checkpoint), so it resumes where it left off instead of skipping"
  say "ahead. This scenario is the argument FOR enabling JobManager HA."
  bold "== Recovery =="
  wait_for_state RUNNING 180
  bold "== Waiting for the fresh job's first completed checkpoint =="
  wait_for_progress 0 120
  status
}

network_partition() {
  status
  local victim prev
  victim=$(busy_taskmanager)
  prev=$(restore_id)
  bold "== FAULT: partitioning $victim off the network =="
  docker network disconnect "$NETWORK" "$victim"
  say "TaskManager process is alive but unreachable. The JobManager only"
  say "notices via heartbeat timeout (heartbeat.timeout, default 50s) —"
  say "watch the job stay RUNNING until detection kicks in, then fail over"
  say "to the standby TaskManager from the last checkpoint."
  bold "== Waiting for heartbeat timeout + failover =="
  wait_for_recovery "$prev" 240
  bold "== Healing partition =="
  docker network connect "$NETWORK" "$victim"
  say "reconnected; TaskManager re-registers as standby capacity."
  sleep 10
  status
}

usage() {
  cat <<EOF
Usage: $0 <scenario>

Scenarios:
  status              show job state, checkpoints, taskmanagers, row counts
  kill-taskmanager    SIGKILL a TaskManager; failover to standby via checkpoint restore
  kafka-outage        stop the broker 30s; restart strategy rides it out
  restart-jobmanager  restart the JobManager; shows why HA exists (state lost, offsets kept)
  network-partition   disconnect a TaskManager; heartbeat-timeout detection then failover
EOF
  exit 1
}

case "${1:-}" in
  status)             status ;;
  kill-taskmanager)   kill_taskmanager ;;
  kafka-outage)       kafka_outage ;;
  restart-jobmanager) restart_jobmanager ;;
  network-partition)  network_partition ;;
  *) usage ;;
esac
