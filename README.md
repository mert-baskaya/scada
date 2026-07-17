# SCADA Grid-Telemetry Demo with Flink CEP

End-to-end streaming pipeline: simulated electrical-distribution components (PLCs/RTUs) → Kafka → Apache Flink (CEP pattern matching + windowed aggregates) → Kafka → Spring Boot API → Postgres + SSE.

New to streaming or SCADA? [GLOSSARY.md](GLOSSARY.md) explains every technical term in this repo in one sentence each, aimed at web developers.

## Quick Start

```bash
cd scada && docker compose up --build -d
```

Wait for all services to be healthy (~60s), then:

```bash
# SSE stream (live aggregates + CEP alerts)
curl -N http://localhost:8096/api/stream

# REST queries
curl -s http://localhost:8096/api/components | jq
curl -s http://localhost:8096/api/aggregates | jq
curl -s http://localhost:8096/api/alerts | jq
curl -s "http://localhost:8096/api/aggregates?componentId=SUB-A-FDR-1&limit=5" | jq
curl -s "http://localhost:8096/api/alerts?componentId=SUB-A-BKR-1&limit=5" | jq

# Direct Kafka verification
docker exec scada-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic scada.alerts \
  --from-beginning
```

## Architecture

```
scada-simulator ──▶ Kafka(scada.telemetry) ──▶ Flink job ──▶ Kafka(scada.aggregates)
                                                       ├──▶ CEP ──▶ Kafka(scada.alerts)
                                                                          │
                             Spring Boot ◀── consumes both ───────────────┘
                             │        │
                       Postgres    SSE stream + REST
```

## Grid Topology

Two substations, 10 components total:

| Substation | Nominal Voltage | Components |
|---|---|---|
| SUB-A | 34.5 kV | XFMR-1, FDR-1, BKR-1, FDR-2, BKR-2 |
| SUB-B | 13.8 kV | XFMR-1, FDR-1, BKR-1, FDR-2, BKR-2 |

Each feeder rated 400 A. Feeders paired with breakers.

## Event Schema — `scada.telemetry`

| Field | Type | Applies To |
|---|---|---|
| componentId | String | all |
| componentType | FEEDER \| TRANSFORMER \| BREAKER | all |
| substationId | String | all |
| voltage | Double (kV) | all |
| current | Double (A) | all |
| frequency | Double (Hz) | all |
| breakerStatus | OPEN \| CLOSED | BREAKER only |
| oilTemp | Double (C) | TRANSFORMER only |
| tapPosition | Integer | TRANSFORMER only |
| timestamp | Instant (ISO-8601) | all |

Non-applicable fields are `null`.

## Fault Scenarios → CEP Patterns

The simulator injects one of four fault scenarios every 45–90 seconds. Flink CEP detects each with a corresponding pattern:

| Scenario | Description | CEP Alert | Keyed By |
|---|---|---|---|
| SAG_THEN_TRIP | Feeder voltage drops to 0.80–0.88 pu for 4–6 readings, then paired breaker opens; recloses ~10 s later | `VOLTAGE_SAG_BREAKER_TRIP` | substationId |
| TRANSFORMER_OVERHEAT | Oil temp ramps +2.5 C/reading for ~20 readings (crosses 90 then 105 C), then cools | `TRANSFORMER_OVERHEAT` | componentId |
| BREAKER_FLAPPING | OPEN/CLOSED toggles every 2–3 readings, 5–6 toggles in ~25 s | `BREAKER_FLAPPING` | componentId |
| OVERCURRENT_BURST | Feeder current 1.3–1.6× rated for 6–8 consecutive readings | `SUSTAINED_OVERCURRENT` | componentId |

CEP patterns use `AfterMatchSkipStrategy.skipPastLastEvent()` per pattern and relaxed contiguity where interleaved events from other components are tolerated.

### CEP Pattern Details

1. **VOLTAGE_SAG_BREAKER_TRIP** — keyed by `substationId` (cross-component correlation)
   - `sag`: FEEDER with `voltage < 0.90 × nominal`, at least 3 occurrences
   - `followedBy` `trip`: BREAKER with status `OPEN`
   - `within(30s)`

2. **TRANSFORMER_OVERHEAT** — keyed by `componentId`
   - `warming`: TRANSFORMER with `90 < oilTemp <= 105` (upper bound prevents re-matching during cool-down)
   - `followedBy` `critical`: TRANSFORMER with `oilTemp > 105`
   - `within(90s)`

3. **BREAKER_FLAPPING** — keyed by `componentId`
   - OPEN → CLOSED → OPEN → CLOSED (relaxed contiguity — repeated same-status readings between toggles are skipped)
   - `within(30s)`

4. **SUSTAINED_OVERCURRENT** — keyed by `componentId`
   - FEEDER with `current > 1.2 × 400A` (480 A), 5 consecutive readings
   - `within(20s)`

## Pipeline Modes & Load Testing

The Flink job runs in one of four modes, selected with the `PIPELINE_MODE` env var
(see [LOAD-TEST.md](LOAD-TEST.md) for the measured numbers and the full story):

| Mode | Detection | Sinks | Measured ceiling (single machine) |
|---|---|---|---|
| `cep` (default) | 4 CEP patterns on the full stream | exactly-once | ~150k ev/s, OOM-collapses at 200k+ |
| `fast` | keyed process functions, one shuffle | at-least-once | 1M ev/s |
| `hybrid` | same CEP patterns behind pre-filters (overcurrent restructured) | exactly-once | 1M ev/s, CEP ops ~0.3% busy |
| `split` | hybrid's detection fed from per-type topics (`scada.telemetry.{transformer,feeder,breaker}`), three parallel sources | exactly-once | 1M ev/s, busiest op ~5% |

`split` also changes the producers: set `TOPIC_MODE=split` on the simulator/load generator so
they publish each device type to its own topic (the two flags must be set together).

```bash
# run the job in hybrid mode
PIPELINE_MODE=hybrid docker compose up -d flink-jobmanager flink-taskmanager flink-taskmanager-2

# blast 1M events/s from a 10,000-component fleet (profile keeps it out of normal runs)
TARGET_EPS=1000000 docker compose --profile loadtest up -d load-generator

# or the whole stack in split mode (per-type topics end to end)
TOPIC_MODE=split PIPELINE_MODE=split docker compose --profile loadtest up -d --build

# measure: produce rate, Flink throughput, consumer lag, operator busyness, checkpoints
./loadtest-report.sh 300 20
```

## Infrastructure Fault Scenarios (Flink Fault Tolerance)

Beyond the simulated *grid* faults above, the stack is configured to demonstrate Flink's own fault tolerance:

- **Checkpointing**: every 10 s, exactly-once mode, stored on a shared volume (`file:///opt/flink/checkpoints`).
- **Restart strategy**: exponential-delay (1 s initial, 30 s max backoff).
- **Two TaskManagers**: with the load-test config (`parallelism.default: 8`) the job spans both, so a killed TaskManager recovers via container restart + checkpoint restore; set `parallelism.default: 1` to reproduce the original idle-standby instant failover.
- **Exactly-once end-to-end**: transactional Kafka sinks + `read_committed` Spring consumer. Results become visible when a checkpoint completes, so aggregates/alerts trail by up to ~10 s.

Inject faults with the harness (each prints before/after evidence — job state, checkpoint restore id, TaskManager registrations, downstream row counts):

```bash
./fault-scenarios.sh status              # baseline health
./fault-scenarios.sh kill-taskmanager    # SIGKILL a TM → failover to standby, state restored from checkpoint
./fault-scenarios.sh kafka-outage        # broker down 30s → restart strategy rides it out
./fault-scenarios.sh restart-jobmanager  # no HA: job resubmits fresh, resumes from committed offsets
./fault-scenarios.sh network-partition   # TM unreachable → heartbeat-timeout detection, then failover
```

| Scenario | What it demonstrates |
|---|---|
| `kill-taskmanager` | Checkpoint-based recovery: tasks redeploy to the standby TaskManager and restore window + CEP state from the last checkpoint (see "LATEST RESTORE" in output / Flink UI). No duplicates downstream thanks to exactly-once sinks. |
| `kafka-outage` | Connector-level resilience: for a short outage the Kafka clients buffer and retry (producer `delivery.timeout.ms` = 2 min), so tasks typically ride it out without restarting — checkpoints stall, then resume. Longer outages fail the tasks and engage the exponential-delay restart strategy. (Simulator uses `acks=0`, so telemetry produced during the outage is dropped at the producer — not a Flink loss.) |
| `restart-jobmanager` | The limitation HA solves: without ZooKeeper/K8s HA, checkpoint metadata is lost and the job restarts fresh — but the Kafka source falls back to offsets committed at the last checkpoint, so it resumes rather than skips ahead. |
| `network-partition` | Failure *detection*: the TaskManager is alive but unreachable; the JobManager notices via heartbeat timeout (~50 s) before the same checkpoint-restore failover as a kill. |

Run them one at a time, with `./fault-scenarios.sh status` showing a healthy job (RUNNING,
checkpoints advancing) between scenarios — stacking them tests recovery-from-recovery, not each
scenario.

## Testing Grid-Fault Scenarios Individually

The simulator picks its four fault scripts **randomly** (one every 45–90 s, `simulator.py`) —
there is no flag to force a specific one. Two ways to test a single CEP pattern:

### Passive: watch one scenario fire

```bash
# terminal 1 — the cause (injection):
docker logs -f scada-simulator | grep "scenario"

# terminal 2 — the effect, filtered to one alert type:
docker exec scada-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic scada.alerts \
  | grep --line-buffered VOLTAGE_SAG_BREAKER_TRIP   # or TRANSFORMER_OVERHEAT / BREAKER_FLAPPING / SUSTAINED_OVERCURRENT
```

All four types cycle through within ~5–10 minutes.

### Deterministic: inject a crafted event sequence

Uses a fake substation `SUB-T` (nominal 34.5 kV — only `SUB-B` is 13.8) so simulator traffic
never interferes, while its background events keep watermarks advancing. Shell helpers:

```bash
emit() {  # emit <kafka-key> <json>
  printf '%s|%s\n' "$1" "$2" | docker exec -i scada-kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic scada.telemetry --property parse.key=true --property key.separator='|'
}
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
```

Expect each alert within ~5–20 s of the last injected event (5 s watermark out-of-orderness
bound + up to one 10 s checkpoint for the exactly-once sink to commit).

**1. VOLTAGE_SAG_BREAKER_TRIP** — 3 FEEDER readings with `voltage < 0.90 × 34.5 = 31.05`, then
a BREAKER `OPEN`, same `substationId`, within 30 s:

```bash
for i in 1 2 3; do
  emit SUB-T-FDR-1 "{\"componentId\":\"SUB-T-FDR-1\",\"componentType\":\"FEEDER\",\"substationId\":\"SUB-T\",\"voltage\":28.0,\"current\":350.0,\"frequency\":60.0,\"breakerStatus\":null,\"oilTemp\":null,\"tapPosition\":null,\"timestamp\":\"$(now)\"}"
  sleep 1
done
emit SUB-T-BKR-1 "{\"componentId\":\"SUB-T-BKR-1\",\"componentType\":\"BREAKER\",\"substationId\":\"SUB-T\",\"voltage\":34.5,\"current\":0.0,\"frequency\":60.0,\"breakerStatus\":\"OPEN\",\"oilTemp\":null,\"tapPosition\":null,\"timestamp\":\"$(now)\"}"
```

**2. TRANSFORMER_OVERHEAT** — one TRANSFORMER reading with `90 < oilTemp ≤ 105`, then one with
`oilTemp > 105`, same component, within 90 s:

```bash
emit SUB-T-XFMR-1 "{\"componentId\":\"SUB-T-XFMR-1\",\"componentType\":\"TRANSFORMER\",\"substationId\":\"SUB-T\",\"voltage\":34.5,\"current\":300.0,\"frequency\":60.0,\"breakerStatus\":null,\"oilTemp\":95.0,\"tapPosition\":0,\"timestamp\":\"$(now)\"}"
sleep 2
emit SUB-T-XFMR-1 "{\"componentId\":\"SUB-T-XFMR-1\",\"componentType\":\"TRANSFORMER\",\"substationId\":\"SUB-T\",\"voltage\":34.5,\"current\":300.0,\"frequency\":60.0,\"breakerStatus\":null,\"oilTemp\":110.0,\"tapPosition\":0,\"timestamp\":\"$(now)\"}"
```

**3. BREAKER_FLAPPING** — OPEN → CLOSED → OPEN → CLOSED on the same breaker within 30 s:

```bash
for st in OPEN CLOSED OPEN CLOSED; do
  emit SUB-T-BKR-2 "{\"componentId\":\"SUB-T-BKR-2\",\"componentType\":\"BREAKER\",\"substationId\":\"SUB-T\",\"voltage\":34.5,\"current\":100.0,\"frequency\":60.0,\"breakerStatus\":\"$st\",\"oilTemp\":null,\"tapPosition\":null,\"timestamp\":\"$(now)\"}"
  sleep 1
done
```

**4. SUSTAINED_OVERCURRENT** — 5 *consecutive* FEEDER readings with `current > 480` within
20 s. Consecutive means no other readings of that component in between — safe here since
`SUB-T-FDR-2` has no other traffic. No sleeps: each `docker exec` already costs ~1 s and all
five must fit inside the 20 s window:

```bash
for i in 1 2 3 4 5; do
  emit SUB-T-FDR-2 "{\"componentId\":\"SUB-T-FDR-2\",\"componentType\":\"FEEDER\",\"substationId\":\"SUB-T\",\"voltage\":34.5,\"current\":520.0,\"frequency\":60.0,\"breakerStatus\":null,\"oilTemp\":null,\"tapPosition\":null,\"timestamp\":\"$(now)\"}"
done
```

**Verify after each injection:**

```bash
# straight from Kafka:
docker exec scada-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic scada.alerts --from-beginning --timeout-ms 5000 | grep SUB-T

# or through the API (proves the full path incl. Postgres):
curl -s 'http://localhost:8096/api/alerts?limit=10' | jq '.[] | {componentId, alertType, timestamp}'

# or live via SSE while injecting:
curl -N http://localhost:8096/api/stream
```

**Split-mode caveat:** with `TOPIC_MODE=split PIPELINE_MODE=split`, point `emit` at the
event type's topic instead — FEEDER → `scada.telemetry.feeder`, BREAKER →
`scada.telemetry.breaker`, TRANSFORMER → `scada.telemetry.transformer`. The sag→trip test then
exercises the cross-topic feeder∪breaker union.

## Services & Ports

| Service | Port | Description |
|---|---|---|
| Flink Web UI | 8095 | Job dashboard, CEP operators visible |
| Spring API | 8096 | REST + SSE endpoints |
| Kafka | 29192 (host) | Message broker (internal: 9092) |
| Postgres | 5475 (host) | Persistence |

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/components` | Distinct component IDs with data |
| GET | `/api/aggregates?componentId=&limit=` | Recent 30 s window aggregates |
| GET | `/api/alerts?componentId=&limit=` | Recent CEP alerts |
| GET | `/api/stream` | SSE (text/event-stream) |

### Aggregate Fields

`componentId`, `readingCount`, `avgVoltage`, `minVoltage`, `maxVoltage`, `avgCurrent`, `maxOilTemp` (nullable), `windowStart`, `windowEnd`

### Alert Fields

`componentId`, `substationId`, `alertType`, `message`, `timestamp`

## Running Alongside Baseline

The SCADA stack uses isolated ports (8095/8096, 29192, 5475) and container names (`scada-*`). The baseline IoT stack can run simultaneously from `../iot-poc` with no conflicts.

## Verification

1. Check simulator logs for scenario lifecycle:
   ```bash
   docker logs -f scada-simulator
   # "scenario started: SAG_THEN_TRIP feeder=SUB-A-FDR-1 breaker=SUB-A-BKR-1"
   # "scenario ended: SAG_THEN_TRIP"
   ```

2. Watch Kafka alerts (should appear within ~1–2 min):
   ```bash
   docker exec scada-kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic scada.alerts
   ```

3. Check Flink UI at http://localhost:8095 — running job with 4 CEP operators plus aggregate window.

All 4 alert types should appear over a few minutes (scenario fires every 45–90 s).

## Build Order

```bash
# Compile Flink job first (fail-fast before Docker build)
mvn -f scada/flink-job/pom.xml package -DskipTests -q

# Then bring up the stack
docker compose -f scada/docker-compose.yml up --build -d
```
