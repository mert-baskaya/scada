package demo.flink;

import demo.flink.model.ComponentAggregate;
import demo.flink.model.GridTelemetry;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class WindowAggregator implements AggregateFunction<GridTelemetry, AggregateAccumulator, AggregateAccumulator> {

    @Override
    public AggregateAccumulator createAccumulator() {
        return new AggregateAccumulator();
    }

    @Override
    public AggregateAccumulator add(GridTelemetry reading, AggregateAccumulator acc) {
        acc.count++;
        if (reading.getVoltage() != null) {
            acc.voltageCount++;
            acc.voltageSum += reading.getVoltage();
            acc.voltageMin = Math.min(acc.voltageMin, reading.getVoltage());
            acc.voltageMax = Math.max(acc.voltageMax, reading.getVoltage());
        }
        if (reading.getCurrent() != null) {
            acc.currentCount++;
            acc.currentSum += reading.getCurrent();
        }
        if (reading.getOilTemp() != null) {
            if (acc.maxOilTemp == null || reading.getOilTemp() > acc.maxOilTemp) {
                acc.maxOilTemp = reading.getOilTemp();
            }
        }
        return acc;
    }

    @Override
    public AggregateAccumulator getResult(AggregateAccumulator acc) {
        return acc;
    }

    @Override
    public AggregateAccumulator merge(AggregateAccumulator a, AggregateAccumulator b) {
        AggregateAccumulator merged = new AggregateAccumulator();
        merged.count = a.count + b.count;
        merged.voltageCount = a.voltageCount + b.voltageCount;
        merged.voltageSum = a.voltageSum + b.voltageSum;
        merged.voltageMin = Math.min(a.voltageMin, b.voltageMin);
        merged.voltageMax = Math.max(a.voltageMax, b.voltageMax);
        merged.currentCount = a.currentCount + b.currentCount;
        merged.currentSum = a.currentSum + b.currentSum;
        if (a.maxOilTemp != null || b.maxOilTemp != null) {
            double ma = a.maxOilTemp != null ? a.maxOilTemp : -Double.MAX_VALUE;
            double mb = b.maxOilTemp != null ? b.maxOilTemp : -Double.MAX_VALUE;
            merged.maxOilTemp = Math.max(ma, mb);
        }
        return merged;
    }

    public static class WindowResultFunction
            extends ProcessWindowFunction<AggregateAccumulator, ComponentAggregate, String, TimeWindow> {

        @Override
        public void process(String componentId, Context ctx, Iterable<AggregateAccumulator> accumulators,
                            Collector<ComponentAggregate> out) {
            AggregateAccumulator acc = accumulators.iterator().next();
            ComponentAggregate result = new ComponentAggregate(
                    componentId,
                    acc.count,
                    round2(acc.voltageCount > 0 ? acc.voltageSum / acc.voltageCount : 0),
                    round2(acc.voltageCount > 0 ? acc.voltageMin : 0),
                    round2(acc.voltageCount > 0 ? acc.voltageMax : 0),
                    round2(acc.currentCount > 0 ? acc.currentSum / acc.currentCount : 0),
                    acc.maxOilTemp != null ? round2(acc.maxOilTemp) : null,
                    Instant.ofEpochMilli(ctx.window().getStart()),
                    Instant.ofEpochMilli(ctx.window().getEnd())
            );
            out.collect(result);
        }

        private static double round2(double v) {
            return Math.round(v * 100.0) / 100.0;
        }
    }
}
