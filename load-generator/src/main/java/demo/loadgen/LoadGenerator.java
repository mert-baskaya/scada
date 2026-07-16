package demo.loadgen;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.LongAdder;

/**
 * High-throughput SCADA telemetry generator. Emits the same JSON schema as
 * scada-simulator/simulator.py but at up to ~1M events/s, spread over a
 * configurable fleet of components, with occasional anomaly episodes that
 * match the thresholds in flink-job's FaultPatterns.
 */
public class LoadGenerator {

    static final double NOMINAL_V = 34.5;
    static final double RATED_CURRENT = 400.0;
    static final int DEVICES_PER_SUBSTATION = 20;

    static final String TOPIC = env("TOPIC", "scada.telemetry");
    static final int TARGET_EPS = Integer.parseInt(env("TARGET_EPS", "1000000"));
    static final int NUM_COMPONENTS = Integer.parseInt(env("NUM_COMPONENTS", "10000"));
    static final int THREADS = Integer.parseInt(env("THREADS", "6"));
    static final int DURATION_SECONDS = Integer.parseInt(env("DURATION_SECONDS", "0"));
    static final int PARTITIONS = Integer.parseInt(env("PARTITIONS", "16"));
    static final String BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");

    static final LongAdder TOTAL_SENT = new LongAdder();
    static volatile boolean running = true;

    static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    enum Kind { TRANSFORMER, FEEDER, BREAKER }

    static final class Component {
        final String id;
        final Kind kind;
        final String jsonPrefix;
        double voltage = NOMINAL_V;
        double current;
        double frequency = 60.0;
        boolean breakerOpen = false;
        double oilTemp;
        int tapPosition = 0;
        int anomalyRemaining = 0;
        int anomalyStep = 0;

        Component(String id, Kind kind, String substation, Random rnd) {
            this.id = id;
            this.kind = kind;
            this.current = 300.0 + rnd.nextDouble() * 150.0;
            this.oilTemp = 55.0 + rnd.nextDouble() * 15.0;
            this.jsonPrefix = "{\"componentId\":\"" + id + "\",\"componentType\":\"" + kind
                    + "\",\"substationId\":\"" + substation + "\",";
        }
    }

    static Component[] buildFleet() {
        int substations = Math.max(1, NUM_COMPONENTS / DEVICES_PER_SUBSTATION);
        Component[] fleet = new Component[NUM_COMPONENTS];
        Random rnd = new Random(42);
        int i = 0;
        outer:
        for (int s = 0; s < substations + 1; s++) {
            String sub = String.format("SUB-%04d", s);
            for (int d = 0; d < DEVICES_PER_SUBSTATION; d++) {
                if (i >= NUM_COMPONENTS) break outer;
                Kind kind;
                String id;
                if (d == 0) {
                    kind = Kind.TRANSFORMER;
                    id = sub + "-XFMR-1";
                } else if (d % 2 == 1) {
                    kind = Kind.FEEDER;
                    id = sub + "-FDR-" + ((d + 1) / 2);
                } else {
                    kind = Kind.BREAKER;
                    id = sub + "-BKR-" + (d / 2);
                }
                fleet[i++] = new Component(id, kind, sub, rnd);
            }
        }
        return fleet;
    }

    public static void main(String[] args) throws Exception {
        System.out.printf("[loadgen] target=%d ev/s components=%d threads=%d partitions=%d duration=%s%n",
                TARGET_EPS, NUM_COMPONENTS, THREADS, PARTITIONS,
                DURATION_SECONDS == 0 ? "unbounded" : DURATION_SECONDS + "s");

        ensureTopic();

        Component[] fleet = buildFleet();
        Thread[] workers = new Thread[THREADS];
        int perThreadRate = TARGET_EPS / THREADS;
        int chunk = fleet.length / THREADS;

        for (int t = 0; t < THREADS; t++) {
            int from = t * chunk;
            int to = (t == THREADS - 1) ? fleet.length : from + chunk;
            Component[] slice = new Component[to - from];
            System.arraycopy(fleet, from, slice, 0, slice.length);
            int rate = (t == THREADS - 1) ? TARGET_EPS - perThreadRate * (THREADS - 1) : perThreadRate;
            workers[t] = new Thread(new Worker(slice, rate), "loadgen-" + t);
            workers[t].start();
        }

        long start = System.nanoTime();
        long lastCount = 0;
        long lastNs = start;
        while (running) {
            Thread.sleep(5000);
            long now = System.nanoTime();
            long count = TOTAL_SENT.sum();
            double rate = (count - lastCount) / ((now - lastNs) / 1e9);
            System.out.printf("[loadgen] rate=%,.0f ev/s total=%,d%n", rate, count);
            lastCount = count;
            lastNs = now;
            if (DURATION_SECONDS > 0 && (now - start) / 1e9 >= DURATION_SECONDS) {
                running = false;
            }
        }
        for (Thread w : workers) w.join();
        double elapsed = (System.nanoTime() - start) / 1e9;
        System.out.printf("[loadgen] done: %,d events in %.1fs (avg %,.0f ev/s)%n",
                TOTAL_SENT.sum(), elapsed, TOTAL_SENT.sum() / elapsed);
    }

    static void ensureTopic() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        try (Admin admin = Admin.create(props)) {
            NewTopic topic = new NewTopic(TOPIC, PARTITIONS, (short) 1)
                    // cap disk use: ~300MB/s ingress would fill the Docker VM disk in minutes
                    .configs(Map.of(
                            "retention.bytes", "536870912",
                            "segment.bytes", "134217728",
                            "retention.ms", "600000"));
            try {
                admin.createTopics(List.of(topic)).all().get();
                System.out.printf("[loadgen] created topic %s with %d partitions%n", TOPIC, PARTITIONS);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) throw e;
                TopicDescription desc = admin.describeTopics(List.of(TOPIC))
                        .allTopicNames().get().get(TOPIC);
                int existing = desc.partitions().size();
                if (existing < PARTITIONS) {
                    System.out.printf("[loadgen] WARNING: topic %s already exists with only %d partition(s); "
                            + "Flink consumption will be capped. Delete it first:%n"
                            + "  docker exec scada-kafka /opt/kafka/bin/kafka-topics.sh "
                            + "--bootstrap-server localhost:9092 --delete --topic %s%n", TOPIC, existing, TOPIC);
                } else {
                    System.out.printf("[loadgen] topic %s exists with %d partitions%n", TOPIC, existing);
                }
            }
        }
    }

    static KafkaProducer<String, byte[]> newProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "0");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, 262144);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        return new KafkaProducer<>(p);
    }

    static final class Worker implements Runnable {
        static final int A_SAG = 1, A_OVERCURRENT = 2, A_BREAKER_OPEN = 3, A_OVERHEAT = 4;

        final Component[] comps;
        final long ratePerSec;
        final Random rnd = new Random();
        final StringBuilder sb = new StringBuilder(512);
        long cachedMillis = -1;
        String cachedTimestamp = "";

        Worker(Component[] comps, long ratePerSec) {
            this.comps = comps;
            this.ratePerSec = ratePerSec;
        }

        @Override
        public void run() {
            KafkaProducer<String, byte[]> producer = newProducer();
            long startNs = System.nanoTime();
            long sent = 0;
            int idx = 0;
            try {
                while (running) {
                    long allowed = (System.nanoTime() - startNs) / 1_000_000 * ratePerSec / 1000;
                    if (sent >= allowed) {
                        LockSupport.parkNanos(500_000);
                        continue;
                    }
                    long batch = Math.min(allowed - sent, 2000);
                    for (long b = 0; b < batch; b++) {
                        Component c = comps[idx];
                        idx = (idx + 1 == comps.length) ? 0 : idx + 1;
                        byte[] value = nextReading(c);
                        producer.send(new ProducerRecord<>(TOPIC, c.id, value));
                    }
                    sent += batch;
                    TOTAL_SENT.add(batch);
                }
            } finally {
                producer.flush();
                producer.close();
            }
        }

        byte[] nextReading(Component c) {
            maybeStartAnomaly(c);
            boolean anomalous = c.anomalyRemaining > 0;
            int anomaly = anomalous ? c.anomalyStep : 0;
            if (anomalous) c.anomalyRemaining--;

            c.voltage += (rnd.nextDouble() - 0.5) * 0.01 * NOMINAL_V;
            c.voltage = clamp(c.voltage, 0.95 * NOMINAL_V, 1.05 * NOMINAL_V);
            c.current += (rnd.nextDouble() - 0.5) * 10.0;
            c.current = clamp(c.current, 250.0, 460.0);
            c.frequency += (rnd.nextDouble() - 0.5) * 0.1;
            c.frequency = clamp(c.frequency, 59.0, 61.0);

            double voltage = c.voltage;
            double current = c.current;
            boolean open = false;
            Double oilTemp = null;
            Integer tap = null;

            switch (c.kind) {
                case TRANSFORMER -> {
                    c.oilTemp += (rnd.nextDouble() - 0.5) * 0.4;
                    c.oilTemp = clamp(c.oilTemp, 45.0, 85.0);
                    oilTemp = c.oilTemp;
                    if (rnd.nextInt(100) == 0) {
                        c.tapPosition = Math.max(-5, Math.min(5, c.tapPosition + (rnd.nextBoolean() ? 1 : -1)));
                    }
                    tap = c.tapPosition;
                    if (anomaly == A_OVERHEAT) {
                        // ramp through the >90 warming band into >105 critical
                        oilTemp = 92.0 + (8 - c.anomalyRemaining) * 2.5;
                    }
                }
                case FEEDER -> {
                    if (anomaly == A_SAG) voltage = NOMINAL_V * (0.80 + rnd.nextDouble() * 0.08);
                    if (anomaly == A_OVERCURRENT) current = RATED_CURRENT * (1.3 + rnd.nextDouble() * 0.3);
                }
                case BREAKER -> {
                    // flapping: alternate OPEN/CLOSED through the episode
                    open = anomaly == A_BREAKER_OPEN && (c.anomalyRemaining % 2 == 0);
                }
            }

            sb.setLength(0);
            sb.append(c.jsonPrefix)
              .append("\"voltage\":").append(round4(voltage))
              .append(",\"current\":").append(round4(current))
              .append(",\"frequency\":").append(round4(c.frequency));
            if (c.kind == Kind.BREAKER) {
                sb.append(",\"breakerStatus\":\"").append(open ? "OPEN" : "CLOSED").append('"');
            } else {
                sb.append(",\"breakerStatus\":null");
            }
            if (oilTemp != null) {
                sb.append(",\"oilTemp\":").append(round4(oilTemp)).append(",\"tapPosition\":").append(tap);
            } else {
                sb.append(",\"oilTemp\":null,\"tapPosition\":null");
            }
            sb.append(",\"timestamp\":\"").append(isoNow()).append("\"}");
            return sb.toString().getBytes(StandardCharsets.ISO_8859_1);
        }

        void maybeStartAnomaly(Component c) {
            if (c.anomalyRemaining > 0 || rnd.nextInt(50_000) != 0) return;
            c.anomalyRemaining = 8;
            c.anomalyStep = switch (c.kind) {
                case FEEDER -> rnd.nextBoolean() ? A_SAG : A_OVERCURRENT;
                case BREAKER -> A_BREAKER_OPEN;
                case TRANSFORMER -> A_OVERHEAT;
            };
        }

        String isoNow() {
            long ms = System.currentTimeMillis();
            if (ms != cachedMillis) {
                cachedMillis = ms;
                cachedTimestamp = Instant.ofEpochMilli(ms).toString();
            }
            return cachedTimestamp;
        }

        static double clamp(double v, double lo, double hi) {
            return v < lo ? lo : Math.min(v, hi);
        }

        static double round4(double v) {
            return Math.round(v * 10000.0) / 10000.0;
        }
    }
}
