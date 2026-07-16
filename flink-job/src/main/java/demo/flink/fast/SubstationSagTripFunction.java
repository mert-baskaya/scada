package demo.flink.fast;

import demo.flink.model.GridAlert;
import demo.flink.model.GridTelemetry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * High-throughput replacement for the SAG_TRIP CEP pattern: three sagging feeder
 * readings followed by a breaker OPEN in the same substation within 30s.
 * Processes events in arrival order, which approximates event-time order across
 * the substation's components at millisecond scale.
 */
public class SubstationSagTripFunction extends KeyedProcessFunction<String, GridTelemetry, GridAlert> {

    private static final double NOMINAL_VOLTAGE_34_5 = 34.5;
    private static final double NOMINAL_VOLTAGE_13_8 = 13.8;
    private static final long SAG_WINDOW_MS = 30_000;

    public static class SagState {
        public int sagCount;
        public long firstSagTs;
        public String feederId;
        public double sagVoltage;
    }

    private transient ValueState<SagState> sag;

    @Override
    public void open(OpenContext openContext) {
        sag = getRuntimeContext().getState(new ValueStateDescriptor<>("sag", SagState.class));
    }

    @Override
    public void processElement(GridTelemetry t, Context ctx, Collector<GridAlert> out) throws Exception {
        long ts = t.getTimestamp().toEpochMilli();
        String type = t.getComponentType();

        if ("FEEDER".equals(type) && t.getVoltage() != null) {
            double nominal = "SUB-B".equals(t.getSubstationId()) ? NOMINAL_VOLTAGE_13_8 : NOMINAL_VOLTAGE_34_5;
            if (t.getVoltage() < 0.90 * nominal) {
                SagState s = sag.value();
                if (s == null) s = new SagState();
                if (s.sagCount == 0 || ts - s.firstSagTs > SAG_WINDOW_MS) {
                    s.sagCount = 1;
                    s.firstSagTs = ts;
                    s.feederId = t.getComponentId();
                    s.sagVoltage = t.getVoltage();
                } else {
                    s.sagCount++;
                }
                sag.update(s);
            }
        } else if ("BREAKER".equals(type) && "OPEN".equals(t.getBreakerStatus())) {
            SagState s = sag.value();
            if (s != null && s.sagCount >= 3 && ts - s.firstSagTs <= SAG_WINDOW_MS) {
                out.collect(new GridAlert(
                        t.getComponentId(), t.getSubstationId(), "VOLTAGE_SAG_BREAKER_TRIP",
                        String.format("Feeder %s sagged %.2f kV then breaker %s tripped OPEN",
                                s.feederId, s.sagVoltage, t.getComponentId()),
                        t.getTimestamp()));
                sag.clear();
            }
        }
    }
}
