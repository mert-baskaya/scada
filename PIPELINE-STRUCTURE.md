# SCADA Pipeline Structure — Visualization Source Summary

Source data for building a visualization of the SCADA demo's architecture. Companion to
`FAULT-SCENARIOS.md`. All values below come from the actual source code and configs
(`simulator.py`, `ScadaStreamingJob.java`, `FaultPatterns.java`, `ResultsListener.java`,
`docker-compose.yml`), not aspirational docs.

---

## 1. End-to-End Flow (the master diagram)

```
┌────────────────┐   scada.telemetry    ┌─────────────────────────────┐   scada.aggregates   ┌────────────────┐      ┌──────────┐
│ scada-simulator│ ───────────────────▶ │        Flink Job            │ ───────────────────▶ │   Spring API   │ ───▶ │ Postgres │
│  (Python)      │   10 events/s        │  1 window branch            │   ~10 rows / 30 s    │  2 listeners   │      └──────────┘
│  10 components │   keyed by           │  4 CEP branches             │                      │  (read_        │ ───▶ SSE stream
└────────────────┘   componentId        │                             │   scada.alerts       │   committed)   │      /api/stream
                                        │                             │ ───────────────────▶ │                │ ───▶ REST
                                        └─────────────────────────────┘   sporadic           └────────────────┘      /api/*
                          ▲ single Kafka broker (KRaft, apache/kafka:3.9.0) carries all 3 topics ▲
```

Six containers total: simulator, kafka, flink-jobmanager, 2× flink-taskmanager, spring-api, postgres
(7 with the standby TaskManager). Ports: Flink UI **8095**, API **8096**, Kafka host **29192**, Postgres **5475**.

---

## 2. Stage 1 — Simulator (data source)

- **Round-robin engine**: one reading per 0.1 s tick, cycling through the 10 components →
  **10 events/s total, 1 event/s per component**. Kafka key = `componentId` (per-component ordering).
- **Producer**: kafka-python, `acks=0` (fire-and-forget — no delivery guarantee upstream of Kafka).
- **Normal behavior**: random walk around nominals (voltage ±0.5%, current ±5 A, frequency clamped 59–61 Hz;
  transformers drift oil temp ±0.2 °C and occasionally move tap position ±1 within [-5, 5]).
- **Scenario engine**: every 45–90 s picks one of 4 grid-fault scripts and overrides the random walk
  for the affected component(s) until the script completes (details in §5).

### Grid topology (good candidate for a substation diagram)

| Substation | Nominal | Components (5 each) |
|---|---|---|
| SUB-A | 34.5 kV | XFMR-1 · FDR-1⇄BKR-1 · FDR-2⇄BKR-2 |
| SUB-B | 13.8 kV | XFMR-1 · FDR-1⇄BKR-1 · FDR-2⇄BKR-2 |

Feeders rated 400 A, each paired with its breaker (⇄ = the pairing used by SAG_THEN_TRIP).

### Telemetry event schema (`scada.telemetry`)

| Field | Type | Applies to |
|---|---|---|
| componentId, componentType, substationId | String / enum | all |
| voltage (kV), current (A), frequency (Hz) | Double | all |
| breakerStatus | OPEN \| CLOSED | BREAKER only (else null) |
| oilTemp (°C), tapPosition (−5..5) | Double / Int | TRANSFORMER only (else null) |
| timestamp | ISO-8601 Instant | all — **event time**, assigned at generation |

---

## 3. Stage 2 — Flink Job (the processing core)

One job, five logical branches off a shared source. Suggested visual: a job-graph
(DAG) with the source fanning out into 1 aggregation lane + 4 CEP lanes, converging into 2 sinks.

### Source & time

| Aspect | Value |
|---|---|
| Source | KafkaSource, topic `scada.telemetry`, group `flink-scada-processor` (in `PIPELINE_MODE=split` + `TOPIC_MODE=split`: three sources on `scada.telemetry.{transformer,feeder,breaker}`, groups `flink-scada-split-*` — see LOAD-TEST.md §7) |
| Start position | committed offsets, fallback `latest` (survives fresh job submission) |
| Watermarks | bounded out-of-orderness **5 s**, idleness **30 s** |
| Time semantics | event time (from the telemetry `timestamp` field) |

### Branch A — windowed aggregation

`keyBy(componentId)` → **tumbling event-time window, 30 s** → incremental `AggregateFunction`
(running count/sum/min/max — O(1) state per key) + `ProcessWindowFunction` (attaches window
start/end) → JSON → **exactly-once Kafka sink** → `scada.aggregates`.

Output per window per component: `readingCount` (~30), `avgVoltage`, `minVoltage`, `maxVoltage`,
`avgCurrent`, `maxOilTemp` (null unless transformer), `windowStart`, `windowEnd`.
Rate: 10 components × 1 window / 30 s = **~10 aggregates per 30 s**.

### Branches B–E — CEP patterns (one lane each)

All use `AfterMatchSkipStrategy.skipPastLastEvent()` (no overlapping matches) and
`followedBy` relaxed contiguity (tolerates interleaved events) except where noted.

| # | Pattern | Keyed by | Structure (from `FaultPatterns.java`) | Window | Emits |
|---|---|---|---|---|---|
| B | Voltage sag → breaker trip | **substationId** (cross-component correlation!) | FEEDER `voltage < 0.90×nominal` ×3 → BREAKER `OPEN` | 30 s | `VOLTAGE_SAG_BREAKER_TRIP` |
| C | Transformer overheat | componentId | `90 < oilTemp ≤ 105` (warming) → `oilTemp > 105` (critical) | 90 s | `TRANSFORMER_OVERHEAT` |
| D | Breaker flapping | componentId | OPEN → CLOSED → OPEN → CLOSED | 30 s | `BREAKER_FLAPPING` |
| E | Sustained overcurrent | componentId | FEEDER `current > 480 A` ×5 **consecutive** (strict contiguity) | 20 s | `SUSTAINED_OVERCURRENT` |

Branch B is the only *cross-component* pattern — the feeder's sag and its breaker's trip are
different devices correlated by substation. Worth highlighting visually.

The four alert streams `union` → JSON → **exactly-once Kafka sink** → `scada.alerts`.

Alert schema: `componentId`, `substationId`, `alertType`, `message`, `timestamp`.

### Fault-tolerance layer (summary — full detail in FAULT-SCENARIOS.md)

Checkpoints every 10 s (exactly-once, ~85 KB state) to a shared volume; exponential-delay restart
(1 s → 30 s); 2 TaskManagers (1 active + 1 standby, parallelism 1); transactional sinks commit
per checkpoint → downstream visibility lags up to ~10 s.

---

## 4. Stage 3 — Spring API (serving layer)

- **Two `@KafkaListener`s** (group `scada-api`, `read_committed`, offset reset `earliest`):
  one per results topic. Each message is (1) persisted via JPA and (2) broadcast to all SSE subscribers.
- **Postgres** (JPA `ddl-auto: update`): `aggregates` and `alerts` tables, each row stamped `receivedAt`.
- **SSE**: `GET /api/stream` — no-timeout emitters in a `CopyOnWriteArrayList`; events named
  `aggregate` and `alert` (plus an initial `connected` event).
- **REST**: `GET /api/components` · `/api/aggregates?componentId=&limit=` · `/api/alerts?componentId=&limit=`.

---

## 5. Grid-Fault Scenario Scripts → CEP Detection (cause → effect pairs)

Suggested visual: paired lanes — simulator injection (cause) above, CEP match (effect) below,
with the detection latency between them.

| Simulator script (cause) | Injected behavior | CEP detection (effect) |
|---|---|---|
| SAG_THEN_TRIP | Feeder at 0.80–0.88 pu for 4–6 readings, then paired breaker OPEN ~10 readings, recloses | Pattern B fires after 3rd sag reading + first OPEN |
| TRANSFORMER_OVERHEAT | Oil temp +2.5 °C/reading for 20 readings (crosses 90 then 105 °C), then cools to ≤65 | Pattern C fires when a >105 °C reading follows a 90–105 °C one |
| BREAKER_FLAPPING | Status toggles every 2–3 readings, 5–6 toggles (~25 s) | Pattern D fires on the 2nd complete OPEN/CLOSED cycle |
| OVERCURRENT_BURST | Feeder at 1.3–1.6× rated (520–640 A) for 6–8 readings | Pattern E fires on the 5th consecutive >480 A reading |

Cadence: one script active at a time, next one 45–90 s after the previous ends.
Since each component emits 1 reading/s, "N readings" ≈ N seconds.

---

## 6. Rates, Latencies & Sizes (annotation numbers)

| Metric | Value | Where it comes from |
|---|---|---|
| Ingest rate | 10 events/s (1/s per component) | simulator 0.1 s tick × round-robin over 10 |
| Aggregate output | ~10 records / 30 s | one window per component |
| Alert output | sporadic; ~1 per scenario, scenario every 45–90 s | scenario engine |
| Event → aggregate visibility | up to ~45 s worst case | 30 s window close + 5 s watermark delay + ≤10 s checkpoint commit |
| Event → alert visibility | pattern completion + ≤5 s watermark + ≤10 s commit | CEP is per-event once the pattern closes |
| Flink state size | ~85 KB per checkpoint | measured (windows + 4 NFA states) |
| Payload sizes | telemetry ~250 B, aggregate ~230 B, alert ~180 B JSON | schemas above |

---

## 7. Suggested Visual Elements

1. **Master flow diagram** (§1): five swim-lane stages with the three Kafka topics as labeled edges; annotate edge throughputs (10/s, ~10/30 s, sporadic).
2. **Flink job DAG** (§3): source → fan-out to 5 branches → 2 sinks; color the aggregation lane vs CEP lanes; badge each CEP node with its key (`substationId` vs `componentId`) and time window.
3. **Grid topology inset** (§2): two substations with feeder⇄breaker pairs and transformers — the physical world the data describes.
4. **Cause→effect scenario lanes** (§5): injection timeline over detection timeline per scenario.
5. **Latency waterfall** (§6): event → window close → watermark → checkpoint commit → Kafka → SSE/Postgres, showing where the up-to-45 s aggregate latency accumulates.
6. **Exactly-once chain callout**: committed offsets → checkpointed state → transactional sink → read_committed consumer, as four linked padlocks across the flow.
