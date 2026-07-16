package demo.flink.hybrid;

import demo.flink.model.GridTelemetry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Emits a breaker reading only when its status differs from the previous one.
 * Breakers report CLOSED almost continuously, so this turns ~45% of the stream
 * into a handful of transition events/s. Equivalent matches for the relaxed
 * flapping pattern, which only advances on alternating statuses anyway.
 */
public class BreakerStatusChangeFunction extends KeyedProcessFunction<String, GridTelemetry, GridTelemetry> {

    private transient ValueState<String> lastStatus;

    @Override
    public void open(OpenContext openContext) {
        lastStatus = getRuntimeContext().getState(new ValueStateDescriptor<>("lastStatus", String.class));
    }

    @Override
    public void processElement(GridTelemetry t, Context ctx, Collector<GridTelemetry> out) throws Exception {
        String status = t.getBreakerStatus();
        if (!status.equals(lastStatus.value())) {
            lastStatus.update(status);
            out.collect(t);
        }
    }
}
