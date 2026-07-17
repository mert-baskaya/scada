# Glossary — Streaming & SCADA Terms for Web Developers

One-sentence explanations of the jargon used in this repo, written for someone who knows web
development but not stream processing or electrical grids. Analogies to familiar web concepts
where they help.

## The Domain (What We're Simulating)

| Term | Explanation |
|---|---|
| SCADA | "Supervisory Control and Data Acquisition" — the industry name for software that monitors and controls physical equipment (power grids, factories, pipelines) by continuously collecting sensor readings. |
| Substation | A fenced-off site in the power grid where high-voltage electricity is converted and routed to neighborhoods; our fleet groups every 20 devices under one substation. |
| Transformer | The device that steps voltage up or down; it's full of insulating oil, and rising oil temperature is the classic early warning that it's about to fail. |
| Feeder | A power line leaving the substation that "feeds" electricity to customers; it has a rated maximum current (400 A here), and exceeding it risks damage. |
| Breaker | A giant circuit breaker, same idea as the one in your apartment: it snaps OPEN to cut power when something is wrong and CLOSED when power flows normally. |
| Voltage sag | A brief dip below normal voltage (like lights dimming), often the first symptom of a fault before a breaker trips. |
| Breaker flapping | A breaker rapidly toggling OPEN/CLOSED/OPEN/CLOSED, which means it can't decide whether the fault is cleared — a sign of a sick device, not a healthy one. |
| Telemetry | The stream of sensor readings (voltage, current, temperature, breaker status) that devices report continuously — in this repo, one JSON message per reading. |
| Nominal voltage | The voltage a component is *supposed* to run at (34.5 kV here); thresholds like "sag = below 90% of nominal" are relative to it. |

## Kafka (The Message Pipe)

| Term | Explanation |
|---|---|
| Kafka | A durable, high-throughput message log that decouples producers from consumers — think of it as a database of append-only event streams rather than a job queue. |
| Broker | A Kafka server process; ours is a single broker, whereas production clusters run several for redundancy. |
| Topic | A named stream of messages (like a table for events); this repo uses `scada.telemetry` in, `scada.aggregates` and `scada.alerts` out. |
| Partition | A topic is split into independent ordered sub-logs so multiple consumers can read in parallel — 16 partitions means at most 16 parallel readers, like sharding a table. |
| Producer / Consumer | The client that writes messages to a topic / the client that reads them, analogous to an HTTP client POSTing versus polling. |
| Message key | An optional label on each message (we use the component ID) that routes all messages with the same key to the same partition, guaranteeing their order relative to each other. |
| Offset | A message's position number within a partition; a consumer tracks "I've read up to offset N" the way a paginated API tracks a cursor. |
| Consumer group | A named set of consumers that share a topic's partitions among themselves and collectively remember their offsets, so a restarted consumer resumes where the group left off. |
| Consumer lag | How far behind a consumer group is (newest offset minus committed offset) — the streaming equivalent of a growing job-queue backlog. |
| Retention | How long (or how many bytes) Kafka keeps old messages before deleting them; it's a rolling window, not a queue that empties when read. |
| acks=0 | A producer setting meaning "fire and forget, don't wait for the broker to confirm" — maximum speed, but messages can be silently lost. |
| KRaft | Kafka's built-in consensus mode that removes the old ZooKeeper dependency; only matters for how the broker runs, not how you use it. |

## Flink (The Stream Processor)

| Term | Explanation |
|---|---|
| Apache Flink | A framework that runs your code continuously over infinite streams of events with managed state and fault tolerance — like a long-lived server process, except the framework handles scaling, crashes, and "what did I already process?" for you. |
| Job | One deployed streaming program (our `ScadaStreamingJob`), which runs forever rather than terminating like a batch script. |
| JobManager | The coordinator node that schedules work and tracks checkpoints — the "control plane" of a Flink cluster. |
| TaskManager | A worker node that actually executes your code; ours have 4 slots each, and killing one is a favorite fault-tolerance demo. |
| Task slot | A fixed slice of a TaskManager that can run one parallel piece of the job — roughly "a worker thread with its own memory budget." |
| Operator | One processing step in the pipeline (parse, filter, window, detect); Flink chains them into a dataflow graph like middleware in a request pipeline. |
| Parallelism | How many copies of each operator run at once (8 here); each copy handles a subset of the data, like horizontal scaling of a service. |
| Source / Sink | The operator that reads from an external system (Kafka in) / writes to one (Kafka out) — the pipeline's input and output adapters. |
| keyBy / shuffle | Partitioning the stream by a field (e.g. component ID) so all events for the same key land on the same operator instance — like consistent-hashing requests to the server that holds that user's session; the resulting network redistribution is called a shuffle. |
| Keyed state | Per-key variables an operator persists between events (a counter per breaker, an accumulator per component) — server-side session storage that Flink snapshots and restores for you. |
| Process function | The low-level "just give me every event and let me manage state and timers myself" API — the escape hatch used by our fast, hybrid, and split modes. |
| Side output | A second, differently-typed output stream from one operator (our detector emits aggregates as the main output and alerts as a side output). |
| Window / Tumbling window | Grouping an infinite stream into finite buckets to aggregate (average, min, max); *tumbling* means fixed, non-overlapping 30-second buckets, like `GROUP BY floor(timestamp/30s)`. |
| Event time vs processing time | Whether logic uses the timestamp *inside* the event (when the sensor measured it) or the wall clock when the machine happens to process it; event time gives correct results even when processing is delayed or replayed. |
| Watermark | Flink's moving marker saying "I believe all events older than time T have arrived," which is what lets it safely close a time window despite events arriving out of order. |
| Out-of-orderness bound | The slack (5 s here) added to watermarks to tolerate late-arriving events — bigger bound means more correctness margin but more buffering and latency. |
| Backpressure | When a slow downstream operator makes upstream operators wait, automatically throttling the whole pipeline instead of dropping data — like TCP flow control for your dataflow. |
| Checkpoint | A consistent snapshot of every operator's state plus the Kafka read positions, taken every 10 s, that the job rewinds to after a crash — a save point in a video game. |
| State backend | Where operator state physically lives — on the JVM heap (fast, RAM-limited, what we use) or in embedded RocksDB (on disk, slower per access, effectively unlimited). |
| Restart strategy | The policy for auto-restarting a failed job (ours: exponential backoff from 1 s to 30 s) — supervisor/systemd restart rules, but for the streaming job. |
| Heartbeat timeout | How long the JobManager waits for silence from a TaskManager before declaring it dead (~50 s); an overloaded, GC-stalled worker misses heartbeats and gets treated as crashed. |
| HA (high availability) | Running standby coordinators so losing the JobManager doesn't lose the job's metadata; this demo deliberately runs without it to show what breaks. |

## CEP (The Pattern Matching)

| Term | Explanation |
|---|---|
| CEP | "Complex Event Processing" — declaring a multi-step pattern over time ("3 sags, then a breaker opens, all within 30 s") and letting the engine find matches in the stream, like a regex where the input is events instead of characters. |
| Pattern | One declared sequence to look for (this repo has four: sag-then-trip, overheat, flapping, overcurrent). |
| Contiguity (relaxed vs consecutive) | Whether unrelated events may sit between the pattern's steps — *relaxed* (`followedBy`) skips them like `a.*b` in regex, while *consecutive* demands they be adjacent like `ab`, which is why consecutive patterns break if you pre-filter the stream. |
| within() | The pattern's time budget — the whole sequence must complete inside it (e.g. 30 s) or the partial match is discarded. |
| AfterMatchSkipStrategy | The rule for what the matcher does after a hit (ours skips past the matched events) so one incident doesn't generate a flood of overlapping alerts. |
| CEP event buffering | The hidden cost of CEP: it must hold *every* incoming event in memory until the watermark confirms nothing older can arrive, so at high rates the buffer, not your logic, is what kills the job. |

## Delivery Guarantees (The "Did It Arrive?" Question)

| Term | Explanation |
|---|---|
| Exactly-once | The end-to-end guarantee that each input event affects the output exactly one time even across crashes and replays — achieved here by committing Kafka output transactions in lockstep with checkpoints. |
| At-least-once | The cheaper guarantee that nothing is lost but replays after a crash may produce duplicates — fine when consumers deduplicate or duplicates are harmless. |
| Transactional sink | A Kafka writer that buffers output in a transaction and commits it only when a checkpoint succeeds, so a crash aborts the half-written output instead of exposing it. |
| read_committed | A consumer setting that hides messages from uncommitted/aborted transactions — the reason downstream sees results in ~10 s steps (one batch per checkpoint) rather than instantly. |
| Idempotent / duplicate-free | The property that applying the same event twice changes nothing; exactly-once pipelines are how you get it without writing dedup logic yourself. |

## Load Testing (The Numbers in LOAD-TEST.md)

| Term | Explanation |
|---|---|
| ev/s (events per second) | Raw message throughput — this repo's benchmark unit; 1M ev/s ≈ 300 MB/s of JSON. |
| Sustained rate | The throughput a system holds indefinitely with healthy checkpoints and non-growing lag, as opposed to a burst it survives briefly. |
| Ceiling | The rate just below where a system stops being sustainable (~150k ev/s for unfiltered CEP mode). |
| Busy time (busyTimeMsPerSecond) | Flink's per-operator utilization metric — 1000 ms/s means that operator instance is saturated and is your bottleneck; 3 ms/s means it's essentially idle. |
| OOM (OutOfMemoryError) | The JVM ran out of heap memory and started killing work — how CEP mode dies under load, because its event buffers grow with input rate. |
| Token bucket | The rate-limiting technique the load generator uses to emit exactly N events/s instead of as-fast-as-possible — the same algorithm behind most API rate limiters. |
| Pre-filtering | Dropping irrelevant events *before* an expensive operator so it only pays for the rare interesting ones — the single change that took CEP from crashing at 200k to idling at 1M. |
| Key skew / hot key | When one key gets disproportionate traffic, overloading the single operator instance that owns it — the streaming version of one celebrity user melting one shard. |
| Backlog / catch-up | The unprocessed events that pile up while a consumer is down or slow, which it must chew through (often at max speed) after recovering. |
