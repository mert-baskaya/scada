# Load Test — 1M events/s Through Four Pipeline Modes

Measured results from driving the SCADA pipeline at up to **1,000,000 events/s** (2026-07-16/17).
All numbers below come from live runs of `./loadtest-report.sh` against the stack on a single
machine (Apple M4 Max, 16 cores, Docker VM with 15.6 GB), not theoretical values.

---

## 1. TL;DR

| Pipeline mode | Sustained rate | At that rate | Failure mode above it |
|---|---|---|---|
| `cep` (default) | ~150k ev/s | 27/27 checkpoints, CEP ~53% busy | 200k+: CEP heap OOM → heartbeat timeout → restart loop |
| `fast` | **1,000,000 ev/s** | 13 ms checkpoints, 1.4 MB state, source ~26% busy | not reached (generator ceiling) |
| `hybrid` | **1,000,000 ev/s** | 39 ms checkpoints, 3 MB state, CEP ops ~0.3% busy | not reached (generator ceiling) |
| `split` | **1,000,000 ev/s** | 23 ms checkpoints, 3 MB state, busiest operator ~5% | not reached (generator ceiling) |

Same detection semantics, same output topics, selected per run:

```bash
PIPELINE_MODE=hybrid docker compose up -d flink-jobmanager flink-taskmanager flink-taskmanager-2

# split mode also changes what the producers do — set both flags together
TOPIC_MODE=split PIPELINE_MODE=split docker compose --profile loadtest up -d --build
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
  minutes. With `TOPIC_MODE=split` it instead creates `scada.telemetry.{transformer,feeder,breaker}`
  (2/8/8 partitions, matching the 5/50/45% fleet volume mix) and routes each component's readings
  to its type's topic.

```bash
TARGET_EPS=1000000 docker compose --profile loadtest up -d load-generator
./loadtest-report.sh 300 20    # 5-minute measured window, 20 s samples
```

Config env vars: `TARGET_EPS` (1,000,000), `NUM_COMPONENTS` (10,000), `THREADS` (6),
`DURATION_SECONDS` (0 = unbounded), `PARTITIONS` (16), `TOPIC_MODE` (`single`; `split` for
per-type topics, with `PARTITIONS_TRANSFORMER/FEEDER/BREAKER` overrides).

The generator held its target within ±0.05% in every run (e.g. `rate=1,000,022 ev/s`), so the
produce side is never the variable being measured.

## 3. Measurement Method

`loadtest-report.sh <duration> <interval>` samples:

- **Produce rate** — the generator's own 5 s counters (measured, not assumed).
- **Flink throughput** — committed-offset growth summed across all `flink-scada-*` consumer
  groups (auto-detected; split mode has three). Earlier versions read `numRecordsOutPerSecond`
  on the source vertex, but that counts a record once per outgoing chained edge — with filters
  chained to the source it reported ~1.95M "ev/s" at a true 1M ingest.
- **Consumer lag** — `kafka-consumer-groups.sh`, lag summed across the same groups. Two caveats:
  a group only exists after the first successful checkpoint (offsets commit on checkpoint),
  and committed offsets trail true consumption by up to one checkpoint interval — at 1M ev/s
  that is ~10M events of apparent "lag" that is pure commit cadence. The per-sample throughput
  column also quantizes (a 20 s sample spans 2 or 3 of the 10 s commits, so it alternates
  ~0.9M/1.36M); trust the summary averages and the lag *sawtooth minimum*, not raw deltas.
- **Operator busyness** — `busyTimeMsPerSecond` per vertex (1000 ms/s = saturated subtask).
- **Checkpoint health** — completed/failed counts, duration, state size.

Every run started from a freshly recreated topic; measuring against leftover backlog skews lag
numbers and (in CEP mode) can OOM the job during catch-up. Topic creation is deterministic: the
`kafka-init` one-shot compose service pre-creates every topic with the right partition counts
before any producer or the Flink job starts, and broker auto-creation is disabled (historically,
whichever client touched a topic first auto-created it with 1 partition and capped consumption).

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

## 7. Split Mode: Per-Type Topics, Parallel Pipelines

`PIPELINE_MODE=split` + `TOPIC_MODE=split` (the flags must be set together — a split job on
single-topic producers consumes nothing, and vice versa) move the event-type routing out of
Flink and into Kafka. Producers write each device type to its own topic; the job runs three
KafkaSources (consumer groups `flink-scada-split-{transformer,feeder,breaker}`, each with its
own watermarks) and feeds every detection branch only the type(s) it can match:

| Branch | Consumes | Operator |
|---|---|---|
| Windowed aggregation | all three topics (union) | unchanged `TumblingEventTimeWindows` |
| OVERHEAT_CEP | `scada.telemetry.transformer` | unchanged pattern behind `PreFilters` |
| FLAPPING_CEP | `scada.telemetry.breaker` | unchanged pattern behind status-change filter |
| OVERCURRENT_DETECTOR | `scada.telemetry.feeder` | hybrid's keyed detector, pre-filter dropped |
| SAG_TRIP_CEP | feeder ∪ breaker (cross-type!) | unchanged pattern, keyed by substationId |

Everything else is hybrid's code reused verbatim — patterns, filters, exactly-once sinks. The
sag→trip branch is why the split can't be total: it correlates a FEEDER sag with a BREAKER trip,
so those two topics union for that branch (union takes the min watermark; the 30 s idleness
setting covers a quiet topic).

**Measured at 1M ev/s** (5-minute window, 2026-07-17): consumption averaged 1,013,993 ev/s
(produce rate plus backlog drain, net lag −14.5k ev/s), **36/36 checkpoints at 23 ms**, 3.0 MB
state, busiest visible operator (SAG_TRIP_FILTER) at ~5%.

**Honest comparison**: at 1M ev/s split ≈ hybrid — both are mostly idle, because hybrid's
pre-filters already solved CEP fan-out inside the job. What split changes is *where* work
happens: the broker routes by type, so no branch deserializes events it will discard, and each
type's consumption scales independently (partitions and parallelism per topic). The difference
would show past hybrid's saturation point, or against `cep` mode's ~150k ceiling.

One operational gotcha, hit in practice: compose caches images, so `--build` is required after
code changes or the load generator silently keeps publishing to the single topic while the env
says split. (A second one — producers racing to auto-create per-type topics with 1 partition —
is fixed structurally: the `kafka-init` one-shot service pre-creates every topic with the right
partition counts before any producer or the Flink job starts, and broker auto-creation is
disabled.)

## 8. What This Means Off a Laptop

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
  feeder stream), fast 2×, split ~1.5× (but the broker has already fanned the ingest into
  per-type topics, so each shuffle moves only that type). On a cluster this is real network,
  not just CPU.
- **Watch key skew**: `keyBy(substationId)` caps that pattern's parallelism at the substation
  count — fine at 500, a hotspot at 10.

## 9. Infrastructure Deltas for the Load Tests

Changes that stay in `docker-compose.yml` (they don't affect the low-rate demo, but note the
last one):

- TaskManagers: 4 slots each, `taskmanager.memory.process.size: 4g`, `parallelism.default: 8`.
- All topics are pre-created by the `kafka-init` one-shot service (telemetry 16 partitions,
  per-type 2/8/8, results topics 1) with broker auto-creation disabled — see §3.
- `PIPELINE_MODE` env passes through the jobmanager (`cep` default).
- **Fault-scenario nuance**: with parallelism 8 the job now occupies all slots on *both*
  TaskManagers, so `kill-taskmanager` no longer fails over to an idle standby — recovery is
  container restart (`restart: always`) + checkpoint restore instead. Set
  `parallelism.default: 1` to reproduce the original standby-failover demo in
  [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md).
