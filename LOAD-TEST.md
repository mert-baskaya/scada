# Load Test — 1M events/s Through Three Pipeline Modes

Measured results from driving the SCADA pipeline at up to **1,000,000 events/s** (2026-07-16).
All numbers below come from live runs of `./loadtest-report.sh` against the stack on a single
machine (Apple M4 Max, 16 cores, Docker VM with 15.6 GB), not theoretical values.

---

## 1. TL;DR

| Pipeline mode | Sustained rate | At that rate | Failure mode above it |
|---|---|---|---|
| `cep` (default) | ~150k ev/s | 27/27 checkpoints, CEP ~53% busy | 200k+: CEP heap OOM → heartbeat timeout → restart loop |
| `fast` | **1,000,000 ev/s** | 13 ms checkpoints, 1.4 MB state, source ~26% busy | not reached (generator ceiling) |
| `hybrid` | **1,000,000 ev/s** | 39 ms checkpoints, 3 MB state, CEP ops ~0.3% busy | not reached (generator ceiling) |

Same detection semantics, same output topics, selected per run:

```bash
PIPELINE_MODE=hybrid docker compose up -d flink-jobmanager flink-taskmanager flink-taskmanager-2
```

## 2. The Load Generator

The Python simulator emits ~10 ev/s — six orders of magnitude short — so `load-generator/` is a
dedicated Java producer (compose profile `loadtest`, normal `docker compose up` unaffected):

- **Fleet**: 10,000 components across 500 substations (1 transformer + feeder/breaker pairs per
  substation), each reporting ~100×/s at the 1M target. Realistic key cardinality for Flink's
  keyed state instead of 10 hot keys.
- **Same JSON schema** the Flink job deserializes, with anomaly episodes (sags, overcurrent
  bursts, breaker flapping, transformer overheat ramps) injected at the thresholds
  `FaultPatterns` matches on, so detection has real work at full rate.
- **Hot path**: N producer threads (default 6) with token-bucket rate control, `acks=0`, lz4,
  256 KB batches, StringBuilder JSON (no Jackson), ISO timestamp cached per millisecond.
- **Topic management**: creates `scada.telemetry` with 16 partitions and capped retention
  (512 MB/partition, 10 min) — at ~300 MB/s ingress an uncapped topic fills the Docker disk in
  minutes.

```bash
TARGET_EPS=1000000 docker compose --profile loadtest up -d load-generator
./loadtest-report.sh 300 20    # 5-minute measured window, 20 s samples
```

Config env vars: `TARGET_EPS` (1,000,000), `NUM_COMPONENTS` (10,000), `THREADS` (6),
`DURATION_SECONDS` (0 = unbounded), `PARTITIONS` (16).

The generator held its target within ±0.05% in every run (e.g. `rate=1,000,022 ev/s`), so the
produce side is never the variable being measured.

## 3. Measurement Method

`loadtest-report.sh <duration> <interval>` samples:

- **Produce rate** — the generator's own 5 s counters (measured, not assumed).
- **Flink throughput** — REST API `numRecordsOutPerSecond` on the source vertex.
- **Consumer lag** — `kafka-consumer-groups.sh` for group `flink-scada-processor`. Two caveats:
  the group only exists after the first successful checkpoint (offsets commit on checkpoint),
  and committed offsets trail true consumption by up to one checkpoint interval — at 1M ev/s
  that is ~10M events of apparent "lag" that is pure commit cadence. Trust the source rate and
  the lag *sawtooth minimum*, not raw lag deltas.
- **Operator busyness** — `busyTimeMsPerSecond` per vertex (1000 ms/s = saturated subtask).
- **Checkpoint health** — completed/failed counts, duration, state size.

Every run started from a freshly recreated topic; measuring against leftover backlog skews lag
numbers and (in CEP mode) can OOM the job during catch-up. Note: the Kafka *consumer* auto-creates
the topic with 1 partition if the job starts before the topic exists — always create it explicitly.

## 4. CEP Mode: Where and Why It Collapses

Stepped runs, 4-minute windows each, parallelism 4, TaskManagers at 4 GB:

| Target rate | Result |
|---|---|
| 50k ev/s | Healthy — 34/34 checkpoints, lag flat |
| 100k ev/s | Healthy — 26/26 checkpoints, CEP ~15% busy, source keeps exact pace |
| 150k ev/s | Healthy — 27/27 checkpoints, CEP ~53% busy, state 347 MB |
| 200k ev/s | Unstable — 21/25 checkpoints failed, task restarts |
| 1M ev/s | Collapse — `OutOfMemoryError` in SAG_TRIP_CEP, 18/18 checkpoints failed, restart loop |

The collapse mechanics: **Flink CEP buffers every event of its keyed stream in heap until the
watermark passes it** (it must sort by event time before running the NFA). Buffer size ≈
rate × out-of-orderness bound (5 s here) × number of CEP operators (4, each with its own full
copy of the stream via its own keyBy shuffle). At 1M ev/s that is tens of millions of live
objects — heap fills, GC stalls, TaskManager heartbeats time out, the job restart-loops, and
each restart makes it worse by adding catch-up backlog. With the original 1.7 GB TaskManagers
this even happened at 200k; 4 GB only moved the cliff.

Also structural, independent of memory: the job shuffles the full stream **five times**
(4 CEP patterns + windowed aggregation). On a real cluster that is 5× the network bandwidth of
the ingest itself.

## 5. Fast Mode: Process Functions

`PIPELINE_MODE=fast` (classes in `flink-job/src/main/java/demo/flink/fast/`) replaces all four
CEP operators with two keyed process functions:

- **`ComponentDetectorFunction`** — keyed by `componentId`; runs overheat, flapping, and
  overcurrent detection **and** the 30 s window aggregation in one operator (alerts leave via a
  side output). Four full-stream shuffles collapse into one. Per-key state is a handful of
  primitives (a timestamp, a stage counter, a window accumulator) instead of buffered events.
- **`SubstationSagTripFunction`** — keyed by `substationId`; sag-count + breaker-trip state
  machine.
- Sinks run **at-least-once** (no Kafka transactions): duplicate alerts are possible on failure
  recovery. Kafka connector 4.0 gotcha: `transactionalIdPrefix` must still be set, unique per
  sink, even for at-least-once.
- Trade-off: detectors process events in per-partition arrival order, not watermark-sorted event
  order. Identical per component (Kafka preserves key order); for the cross-component sag-trip
  pattern, millisecond-scale reordering between a substation's devices is possible.

**Measured at 1M ev/s** (5-minute window, parallelism 8): source locked to 1,000,0xx ev/s for
3.5 min straight after warmup, 37/37 checkpoints at 13 ms, 1.4 MB state, busiest operator (the
source) at ~26%. Aggregates provably complete: 3002 readings per component per 30 s window ×
10,000 components = 1M ev/s exactly. All four alert types verified on `scada.alerts`.

## 6. Hybrid Mode: Real CEP Behind Pre-Filters

`PIPELINE_MODE=hybrid` (classes in `flink-job/src/main/java/demo/flink/hybrid/`) is the
recommended real-world shape: **keep the CEP `Pattern` definitions unchanged, but never show
them the firehose.** Each pattern only matches on rare events, so filter to those first:

| Pattern | Pre-filter | CEP input at 1M ev/s |
|---|---|---|
| SAG_TRIP_CEP (unchanged) | feeder with `voltage < 0.9 × nominal` OR breaker `OPEN` | ~hundreds/s |
| OVERHEAT_CEP (unchanged) | transformer with `oilTemp > 90` | ~tens/s |
| FLAPPING_CEP (unchanged) | breaker readings → `BreakerStatusChangeFunction` keeps only status *transitions* (breakers report CLOSED continuously) | ~tens/s |
| SUSTAINED_OVERCURRENT | **cannot be pre-filtered** — `times(5).consecutive()` is defined over *every* reading of the feeder, so dropping events changes its meaning. Restructured as `OvercurrentDetector`, a keyed process function with the same thresholds. | n/a |

Pre-filtering is semantics-preserving for the three surviving patterns because they use relaxed
contiguity (`followedBy`, non-consecutive `times`) — CEP skips non-matching events anyway; the
filter just removes them before they are buffered. Watermarks flow through filters untouched, so
event-time behavior (including `within()` windows) is identical. Aggregation stays the original
`TumblingEventTimeWindows` path and the sinks stay **exactly-once** — unlike fast mode, no
delivery-guarantee trade-off.

**Measured at 1M ev/s** (5-minute window): consumption locked to produce rate, lag *draining*
(net −17k ev/s), 33/33 checkpoints at 39 ms, 3 MB state. Operator busyness tells the story:

| Operator | busyTimeMsPerSecond (avg/subtask) |
|---|---|
| Source + 4 chained pre-filters | 305 |
| Window aggregation → exactly-once sink | 99 |
| STATUS_CHANGE_FILTER | 42 |
| OVERCURRENT_DETECTOR | 37 |
| FLAPPING_CEP | 4 |
| SAG_TRIP_CEP | 3 |
| OVERHEAT_CEP | 3 |

The same CEP operators that crashed the job at 200k ev/s are ~0.3% busy at 5× that rate.
Alert mix over a 300-alert sample: 207 flapping, 59 overcurrent, 18 sag-trip, 16 overheat —
all four types flowing through the unchanged patterns.

## 7. What This Means Off a Laptop

- **CEP scales horizontally** (it is keyed), but brute-forcing 1M ev/s through unfiltered CEP
  costs roughly 10× the hardware of the hybrid shape — and RocksDB, the usual OOM fix, trades
  the crash for 2–5× lower per-core CEP throughput because of the shared buffer's
  serialization pattern.
- **Pre-filtering is the highest-leverage change** (10–50×): CEP cost is proportional to what it
  buffers, not what the pipeline ingests. Check contiguity semantics per pattern before
  filtering — `consecutive()` patterns must be restructured instead.
- **Watermark tightness is the second lever**: CEP buffer ∝ rate × out-of-orderness. The demo
  allows 5 s; a well-behaved transport justifies 500 ms–1 s.
- **Count your shuffles**: cep mode moves the full stream 5×, hybrid ~1.45× (aggregation +
  feeder stream), fast 2×. On a cluster this is real network, not just CPU.
- **Watch key skew**: `keyBy(substationId)` caps that pattern's parallelism at the substation
  count — fine at 500, a hotspot at 10.

## 8. Infrastructure Deltas for the Load Tests

Changes that stay in `docker-compose.yml` (they don't affect the low-rate demo, but note the
last one):

- TaskManagers: 4 slots each, `taskmanager.memory.process.size: 4g`, `parallelism.default: 8`.
- `scada.telemetry` should be pre-created with 16 partitions (see §3 on consumer auto-create).
- `PIPELINE_MODE` env passes through the jobmanager (`cep` default).
- **Fault-scenario nuance**: with parallelism 8 the job now occupies all slots on *both*
  TaskManagers, so `kill-taskmanager` no longer fails over to an idle standby — recovery is
  container restart (`restart: always`) + checkpoint restore instead. Set
  `parallelism.default: 1` to reproduce the original standby-failover demo in
  [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md).
