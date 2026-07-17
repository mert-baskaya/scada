package demo.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import demo.flink.cep.AlertFunctions;
import demo.flink.cep.FaultPatterns;
import demo.flink.fast.ComponentDetectorFunction;
import demo.flink.fast.SubstationSagTripFunction;
import demo.flink.hybrid.BreakerStatusChangeFunction;
import demo.flink.hybrid.OvercurrentDetector;
import demo.flink.hybrid.PreFilters;
import demo.flink.model.ComponentAggregate;
import demo.flink.model.GridAlert;
import demo.flink.model.GridTelemetry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

public class ScadaStreamingJob {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String checkpointDir = System.getenv().getOrDefault("CHECKPOINT_DIR", "file:///opt/flink/checkpoints");

        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofSeconds(1));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofSeconds(30));
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        String pipelineMode = System.getenv().getOrDefault("PIPELINE_MODE", "cep");
        boolean fast = "fast".equalsIgnoreCase(pipelineMode);
        boolean hybrid = "hybrid".equalsIgnoreCase(pipelineMode);
        boolean split = "split".equalsIgnoreCase(pipelineMode);
        if (fast || hybrid || split) {
            env.getConfig().enableObjectReuse();
        }

        KafkaSink<String> aggregateSink = jsonSink(bootstrapServers, "scada.aggregates", !fast, "scada-aggregates");
        KafkaSink<String> alertSink = jsonSink(bootstrapServers, "scada.alerts", !fast, "scada-alerts");

        if (split) {
            runSplitPipeline(env, bootstrapServers, aggregateSink, alertSink);
            return;
        }

        DataStream<GridTelemetry> readings = env
                .fromSource(telemetrySource(bootstrapServers, "scada.telemetry", "flink-scada-processor"),
                        telemetryWatermarks(), "Kafka Source");

        if (fast) {
            var detected = readings
                    .keyBy(GridTelemetry::getComponentId)
                    .process(new ComponentDetectorFunction())
                    .name("UNIFIED_DETECTOR");

            detected
                    .map(new AggregateToJson())
                    .sinkTo(aggregateSink)
                    .name("Aggregate Sink");

            DataStream<GridAlert> sagAlerts = readings
                    .keyBy(GridTelemetry::getSubstationId)
                    .process(new SubstationSagTripFunction())
                    .name("SAG_TRIP_DETECTOR");

            detected.getSideOutput(ComponentDetectorFunction.ALERTS)
                    .union(sagAlerts)
                    .map(new AlertToJson())
                    .sinkTo(alertSink)
                    .name("Alert Sink");

            env.execute("SCADA Streaming Job (fast path)");
            return;
        }

        readings
                .keyBy(GridTelemetry::getComponentId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
                .aggregate(new WindowAggregator(), new WindowAggregator.WindowResultFunction())
                .map(new AggregateToJson())
                .sinkTo(aggregateSink)
                .name("Aggregate Sink");

        if (hybrid) {
            DataStream<GridAlert> sagAlerts = CEP.pattern(
                    readings.filter(PreFilters::sagTripRelevant).name("SAG_TRIP_FILTER")
                            .keyBy(GridTelemetry::getSubstationId),
                    FaultPatterns.voltageSagBreakerTrip()
            ).inEventTime().process(AlertFunctions.sagTripProcessor()).name("SAG_TRIP_CEP");

            DataStream<GridAlert> overheatAlerts = CEP.pattern(
                    readings.filter(PreFilters::overheatRelevant).name("OVERHEAT_FILTER")
                            .keyBy(GridTelemetry::getComponentId),
                    FaultPatterns.transformerOverheat()
            ).inEventTime().process(AlertFunctions.overheatProcessor()).name("OVERHEAT_CEP");

            DataStream<GridAlert> flappingAlerts = CEP.pattern(
                    readings.filter(PreFilters::breakerReading).name("BREAKER_FILTER")
                            .keyBy(GridTelemetry::getComponentId)
                            .process(new BreakerStatusChangeFunction()).name("STATUS_CHANGE_FILTER")
                            .keyBy(GridTelemetry::getComponentId),
                    FaultPatterns.breakerFlapping()
            ).inEventTime().process(AlertFunctions.flappingProcessor()).name("FLAPPING_CEP");

            DataStream<GridAlert> overcurrentAlerts = readings
                    .filter(PreFilters::feederReading).name("FEEDER_FILTER")
                    .keyBy(GridTelemetry::getComponentId)
                    .process(new OvercurrentDetector())
                    .name("OVERCURRENT_DETECTOR");

            sagAlerts.union(overheatAlerts, flappingAlerts, overcurrentAlerts)
                    .map(new AlertToJson())
                    .sinkTo(alertSink)
                    .name("Alert Sink");

            env.execute("SCADA Streaming Job (hybrid CEP)");
            return;
        }

        DataStream<GridAlert> sagAlerts = CEP.pattern(
                readings.keyBy(GridTelemetry::getSubstationId),
                FaultPatterns.voltageSagBreakerTrip()
        ).inEventTime().process(AlertFunctions.sagTripProcessor()).name("SAG_TRIP_CEP");

        DataStream<GridAlert> overheatAlerts = CEP.pattern(
                readings.keyBy(GridTelemetry::getComponentId),
                FaultPatterns.transformerOverheat()
        ).inEventTime().process(AlertFunctions.overheatProcessor()).name("OVERHEAT_CEP");

        DataStream<GridAlert> flappingAlerts = CEP.pattern(
                readings.keyBy(GridTelemetry::getComponentId),
                FaultPatterns.breakerFlapping()
        ).inEventTime().process(AlertFunctions.flappingProcessor()).name("FLAPPING_CEP");

        DataStream<GridAlert> overcurrentAlerts = CEP.pattern(
                readings.keyBy(GridTelemetry::getComponentId),
                FaultPatterns.sustainedOvercurrent()
        ).inEventTime().process(AlertFunctions.overcurrentProcessor()).name("OVERCURRENT_CEP");

        sagAlerts.union(overheatAlerts, flappingAlerts, overcurrentAlerts)
                .map(new AlertToJson())
                .sinkTo(alertSink)
                .name("Alert Sink");

        env.execute("SCADA Streaming Job with CEP");
    }

    private static KafkaSink<String> jsonSink(String bootstrapServers, String topic,
                                              boolean exactlyOnce, String txnPrefix) {
        var builder = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                // required to be unique per sink even for at-least-once delivery
                .setTransactionalIdPrefix(txnPrefix)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build());
        if (exactlyOnce) {
            builder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    // Flink's default (1h) exceeds the broker's transaction.max.timeout.ms (15m)
                    .setProperty("transaction.timeout.ms", "600000");
        } else {
            builder.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
        }
        return builder.build();
    }

    private static KafkaSource<GridTelemetry> telemetrySource(String bootstrap, String topic, String groupId) {
        return KafkaSource.<GridTelemetry>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new GridTelemetryDeserializer()))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();
    }

    private static WatermarkStrategy<GridTelemetry> telemetryWatermarks() {
        return WatermarkStrategy
                .<GridTelemetry>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getTimestamp().toEpochMilli())
                .withIdleness(Duration.ofSeconds(30));
    }

    private static void runSplitPipeline(StreamExecutionEnvironment env, String bootstrapServers,
                                          KafkaSink<String> aggregateSink, KafkaSink<String> alertSink) throws Exception {
        DataStream<GridTelemetry> transformers = env
                .fromSource(telemetrySource(bootstrapServers, "scada.telemetry.transformer",
                        "flink-scada-split-transformer"), telemetryWatermarks(), "Kafka Source (transformer)");
        DataStream<GridTelemetry> feeders = env
                .fromSource(telemetrySource(bootstrapServers, "scada.telemetry.feeder",
                        "flink-scada-split-feeder"), telemetryWatermarks(), "Kafka Source (feeder)");
        DataStream<GridTelemetry> breakers = env
                .fromSource(telemetrySource(bootstrapServers, "scada.telemetry.breaker",
                        "flink-scada-split-breaker"), telemetryWatermarks(), "Kafka Source (breaker)");

        transformers.union(feeders, breakers)
                .keyBy(GridTelemetry::getComponentId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
                .aggregate(new WindowAggregator(), new WindowAggregator.WindowResultFunction())
                .map(new AggregateToJson())
                .sinkTo(aggregateSink)
                .name("Aggregate Sink");

        DataStream<GridAlert> overheatAlerts = CEP.pattern(
                transformers.filter(PreFilters::overheatRelevant).name("OVERHEAT_FILTER")
                        .keyBy(GridTelemetry::getComponentId),
                FaultPatterns.transformerOverheat()
        ).inEventTime().process(AlertFunctions.overheatProcessor()).name("OVERHEAT_CEP");

        DataStream<GridAlert> sagAlerts = CEP.pattern(
                feeders.union(breakers).filter(PreFilters::sagTripRelevant).name("SAG_TRIP_FILTER")
                        .keyBy(GridTelemetry::getSubstationId),
                FaultPatterns.voltageSagBreakerTrip()
        ).inEventTime().process(AlertFunctions.sagTripProcessor()).name("SAG_TRIP_CEP");

        DataStream<GridAlert> flappingAlerts = CEP.pattern(
                breakers.filter(PreFilters::breakerReading).name("BREAKER_FILTER")
                        .keyBy(GridTelemetry::getComponentId)
                        .process(new BreakerStatusChangeFunction()).name("STATUS_CHANGE_FILTER")
                        .keyBy(GridTelemetry::getComponentId),
                FaultPatterns.breakerFlapping()
        ).inEventTime().process(AlertFunctions.flappingProcessor()).name("FLAPPING_CEP");

        DataStream<GridAlert> overcurrentAlerts = feeders
                .keyBy(GridTelemetry::getComponentId)
                .process(new OvercurrentDetector())
                .name("OVERCURRENT_DETECTOR");

        overheatAlerts.union(sagAlerts, flappingAlerts, overcurrentAlerts)
                .map(new AlertToJson())
                .sinkTo(alertSink)
                .name("Alert Sink");

        env.execute("SCADA Streaming Job (split topics)");
    }

    private static class AggregateToJson implements MapFunction<ComponentAggregate, String> {
        @Override
        public String map(ComponentAggregate value) throws Exception {
            return MAPPER.writeValueAsString(value);
        }
    }

    private static class AlertToJson implements MapFunction<GridAlert, String> {
        @Override
        public String map(GridAlert value) throws Exception {
            return MAPPER.writeValueAsString(value);
        }
    }

    private static class GridTelemetryDeserializer implements DeserializationSchema<GridTelemetry> {

        @Override
        public GridTelemetry deserialize(byte[] message) {
            try {
                return MAPPER.readValue(message, GridTelemetry.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize GridTelemetry", e);
            }
        }

        @Override
        public boolean isEndOfStream(GridTelemetry nextElement) {
            return false;
        }

        @Override
        public TypeInformation<GridTelemetry> getProducedType() {
            return TypeInformation.of(GridTelemetry.class);
        }
    }
}
