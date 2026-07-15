# Fault-Tolerance Scenarios — Visualization Source Summary

Source data for building a visualization of the SCADA demo's infrastructure fault scenarios.
All timings and counts below were **measured on live runs** of `./fault-scenarios.sh` (2026-07-15),
not theoretical values.

---

## 1. System Under Test

### Pipeline topology (left-to-right flow diagram)

```
scada-simulator ──▶ Kafka [scada.telemetry] ──▶ Flink Job ──▶ Kafka [scada.aggregates] ──▶ Spring API ──▶ Postgres + SSE
   (acks=0)                                        │
                                                   └──▶ 4× CEP ──▶ Kafka [scada.alerts] ──┘
```

### Flink cluster (the fault-injection target)

| Element | Value | Role in the story |
|---|---|---|
| JobManager | `scada-flink-jobmanager` (1×, no HA) | Coordinator; single point of failure by design |
| TaskManager A | `scada-flink-taskmanager`, 2 slots | One TM hosts the whole job (parallelism 1) |
| TaskManager B | `scada-flink-taskmanager-2`, 2 slots | Standby capacity → instant failover target |
| Checkpoint storage | shared volume `file:///opt/flink/checkpoints` | Survives container death; both TMs + JM mount it |

### Fault-tolerance configuration

| Mechanism | Setting | Visual cue suggestion |
|---|---|---|
| Checkpoint interval | 10 s, exactly-once mode | Tick marks on every timeline every 10 s |
| Checkpoint state size | ~84–87 KB per checkpoint (measured) | — |
| Restart strategy | exponential-delay, 1 s → 30 s max backoff | — |
| Heartbeat timeout | 50 s (Flink default) | Key constant for the partition scenario |
| Kafka producer resilience | `delivery.timeout.ms` = 2 min (client default) | Key constant for the outage scenario |
| Sink delivery | Kafka transactions, committed per checkpoint | Explains ~10 s visibility lag downstream |
| Consumer isolation | Spring API reads `read_committed` | Aborted transactions never reach Postgres/SSE |
| Source offsets | committed at each checkpoint; fresh jobs resume from them | Key for the JobManager scenario |

---

## 2. The Four Scenarios (one timeline lane each)

Recommended visual: horizontal timelines, x = seconds since fault injection, with phases
DETECTION → RECOVERY → HEALTHY colored distinctly. Each scenario differs in *what* detects
the fault, *what* is lost, and *how long* detection takes — that contrast is the story.

### Scenario 1 — `kill-taskmanager` (process crash)

**Fault**: SIGKILL the TaskManager hosting the job (script auto-targets the busy TM via REST API).
**Detection mechanism**: TCP connection loss — fast.

Measured timeline:

| t (s) | Event |
|---|---|
| 0 | SIGKILL delivered; job still *reports* RUNNING (stale) |
| ~0–20 | JobManager detects lost TM, cancels tasks, requests slots on standby TM |
| ~25 | Job RUNNING on TaskManager B, state **restored from checkpoint 24** |
| ~35 | Killed container restarted (explicit `docker start` — `restart: always` does NOT cover `docker kill`), re-registers as new standby |

**Lost**: at most 10 s of processing (replayed from Kafka since last checkpoint). **Kept**: all window + CEP state, offsets. **Duplicates downstream**: 0.
Evidence marker: Flink REST `latest.restored.id` changed → "LATEST RESTORE: checkpoint id=24".

### Scenario 2 — `network-partition` (TM alive but unreachable)

**Fault**: `docker network disconnect` on the busy TaskManager. Process keeps running.
**Detection mechanism**: heartbeat timeout — slow. This is the contrast with scenario 1.

Measured timeline:

| t (s) | Event |
|---|---|
| 0 | Partition begins; job reports RUNNING throughout detection |
| 0–45 | *Silent zone*: JobManager still believes the TM is healthy (heartbeats missing but not yet timed out) |
| ~50 | Heartbeat timeout fires → same failover path as a kill; restored from checkpoint 3 on the other TM |
| ~50+ | Partition healed; TM re-registers as standby |

**Visualization key point**: identical recovery to scenario 1, but detection took ~50 s vs ~20 s — annotate the heartbeat-timeout window as the dominant cost.

### Scenario 3 — `kafka-outage` (dependency outage, 30 s)

**Fault**: `docker stop scada-kafka` for 30 s.
**Detection mechanism**: none needed — **the job never restarted**.

Measured timeline:

| t (s) | Event |
|---|---|
| 0 | Broker down. Source reads nothing; sink transactions cannot commit |
| 0–30 | Kafka clients buffer & retry internally (producer delivery timeout 2 min ≫ 30 s outage). Job state: RUNNING the whole time. Checkpoints stall/fail (2 failed checkpoints recorded) |
| 30 | Broker restarted |
| ~30–40 | First fresh completed checkpoint → proves end-to-end health restored |

**Lesson for the visual**: resilience is layered — the *connector retry layer* absorbed this fault before the *restart strategy* was ever needed. `latest.restored` did **not** change (no restore happened).
Caveat to annotate: simulator produces with `acks=0`, so telemetry generated during the outage was dropped at the producer (upstream loss, not a Flink loss).

### Scenario 4 — `restart-jobmanager` (coordinator loss, no HA)

**Fault**: `docker restart scada-flink-jobmanager`.
**This is the designed failure case** — it shows what HA (ZooKeeper/K8s) would prevent.

Measured timeline:

| t (s) | Event |
|---|---|
| 0 | JM down; REST API unreachable; TMs keep running but leaderless |
| ~5 | JM back; standalone-job mode resubmits the job **fresh** |
| ~5–50 | New execution spins up; checkpoint counter resets (first completed checkpoint = id 1 at ~t=45) |

**Lost**: all window + CEP state (checkpoint metadata lived only in JM memory — no HA). In-flight windows/partial CEP matches vanish.
**Kept**: Kafka offsets (committed at each checkpoint) → source resumed where it left off rather than skipping ahead. Downstream row count kept growing (200 → 220) with no gap-flood and no duplicates.
Evidence marker: **no** "LATEST RESTORE" line + checkpoint ids restarting from 1.

---

## 3. Cross-Scenario Comparison (good candidate for a summary matrix/chart)

| | kill-taskmanager | network-partition | kafka-outage | restart-jobmanager |
|---|---|---|---|---|
| Detection mechanism | TCP connection loss | Heartbeat timeout | (none — clients retry) | Container restart |
| Detection time | ~20 s | **~50 s** | n/a | ~5 s |
| Time to healthy | ~25 s | ~50 s | ~10 s after broker back | ~45 s (first fresh checkpoint) |
| Job restarted? | yes (failover) | yes (failover) | **no** | yes (fresh submission) |
| Checkpoint restore? | yes | yes | no | **no (state lost)** |
| State preserved? | ✅ | ✅ | ✅ (never lost) | ❌ |
| Offsets preserved? | ✅ (in checkpoint) | ✅ (in checkpoint) | ✅ | ✅ (committed offsets fallback) |
| Duplicates downstream | 0 | 0 | 0 | 0 |
| What it teaches | Checkpoint recovery + standby failover | Detection latency = heartbeat cost | Layered resilience below the restart strategy | Why JobManager HA exists |

### Aggregate evidence after ALL four scenarios (run back-to-back)

- **260** aggregate rows in Postgres, **0 duplicate** (componentId, windowStart) pairs → exactly-once held through every fault.
- Alerts kept firing across faults (1 → 9 over the session).
- Both TaskManagers registered and healthy at the end; job RUNNING with checkpoints completing every 10 s.

---

## 4. Suggested Visual Elements

1. **Four horizontal timelines** (shared x-axis, seconds), phases colored: fault (red mark) → undetected/stale-RUNNING (hatched) → recovering (amber) → healthy (green). Checkpoint ticks every 10 s along the top.
2. **Detection-time bar comparison**: 5 s (JM restart) / ~20 s (kill) / ~50 s (partition) / ∞-not-needed (Kafka) — the single most quotable contrast.
3. **State-preservation matrix**: scenarios × {state, offsets, duplicates} with ✅/❌ — only JM restart loses state.
4. **Topology diagram with failover arrow**: job moving from TM-A to TM-B, checkpoint volume shown as the shared artifact both read/write; "restore" arrow from volume to TM-B.
5. **Evidence footer**: `260 rows / 0 duplicates` as the exactly-once proof point.

Data sources if live/interactive: Flink REST (`:8095/jobs/overview`, `/jobs/<id>/checkpoints`, `/taskmanagers`), Spring API (`:8096/api/aggregates`, `/api/alerts`), or `./fault-scenarios.sh status`.
