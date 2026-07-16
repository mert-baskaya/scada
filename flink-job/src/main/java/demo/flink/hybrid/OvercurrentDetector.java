package demo.flink.hybrid;

import demo.flink.model.GridAlert;
import demo.flink.model.GridTelemetry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Replaces the sustainedOvercurrent CEP pattern. Its times(5).consecutive()
 * contiguity is defined over every reading of the feeder, so it cannot sit
 * behind a pre-filter like the other patterns; instead the consecutive count is
 * tracked here with a few bytes of state. Thresholds mirror FaultPatterns.
 */
public class OvercurrentDetector extends KeyedProcessFunction<String, GridTelemetry, GridAlert> {

    private static final double RATED_CURRENT = 400.0;
    private static final long WINDOW_MS = 20_000;
    private static final int REQUIRED_COUNT = 5;

    public static class OcState {
        public int count;
        public long firstTs;
        public double maxCurrent;
    }

    private transient ValueState<OcState> state;

    @Override
    public void open(OpenContext openContext) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("overcurrent", OcState.class));
    }

    @Override
    public void processElement(GridTelemetry t, Context ctx, Collector<GridAlert> out) throws Exception {
        Double current = t.getCurrent();
        if (current == null) return;
        OcState o = state.value();
        if (current > 1.2 * RATED_CURRENT) {
            long ts = t.getTimestamp().toEpochMilli();
            if (o == null) o = new OcState();
            if (o.count == 0 || ts - o.firstTs > WINDOW_MS) {
                o.count = 1;
                o.firstTs = ts;
                o.maxCurrent = current;
            } else {
                o.count++;
                o.maxCurrent = Math.max(o.maxCurrent, current);
            }
            if (o.count >= REQUIRED_COUNT) {
                out.collect(new GridAlert(
                        t.getComponentId(), t.getSubstationId(), "SUSTAINED_OVERCURRENT",
                        String.format("Feeder %s sustained overcurrent: %d readings above 480 A, max %.1f A",
                                t.getComponentId(), o.count, o.maxCurrent),
                        t.getTimestamp()));
                o.count = 0;
            }
            state.update(o);
        } else if (o != null && o.count > 0) {
            state.clear();
        }
    }
}
