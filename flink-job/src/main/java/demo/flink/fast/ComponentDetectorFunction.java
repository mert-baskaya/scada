package demo.flink.fast;

import demo.flink.model.ComponentAggregate;
import demo.flink.model.GridAlert;
import demo.flink.model.GridTelemetry;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

/**
 * High-throughput replacement for the OVERHEAT/FLAPPING/OVERCURRENT CEP patterns
 * plus the 30s tumbling window aggregation, all in a single keyed operator so the
 * stream is shuffled once instead of four times. Keeps a few bytes of state per
 * component instead of CEP's per-event buffers. Detection thresholds and time
 * bounds mirror demo.flink.cep.FaultPatterns.
 */
public class ComponentDetectorFunction extends KeyedProcessFunction<String, GridTelemetry, ComponentAggregate> {

    public static final OutputTag<GridAlert> ALERTS =
            new OutputTag<>("component-alerts", TypeInformation.of(GridAlert.class));

    private static final double RATED_CURRENT = 400.0;
    private static final long WINDOW_MS = 30_000;
    private static final long OVERHEAT_WINDOW_MS = 90_000;
    private static final long FLAPPING_WINDOW_MS = 30_000;
    private static final long OVERCURRENT_WINDOW_MS = 20_000;

    public static class WinAcc {
        public int count;
        public int voltageCount;
        public double voltageSum;
        public double voltageMin = Double.MAX_VALUE;
        public double voltageMax = -Double.MAX_VALUE;
        public int currentCount;
        public double currentSum;
        public double maxOilTemp = -Double.MAX_VALUE;
        public boolean hasOilTemp;
    }

    public static class FlapState {
        public int stage;
        public long firstTs;
    }

    public static class OcState {
        public int count;
        public long firstTs;
        public double maxCurrent;
    }

    private transient MapState<Long, WinAcc> windows;
    private transient ValueState<Long> warmingTs;
    private transient ValueState<FlapState> flap;
    private transient ValueState<OcState> overcurrent;

    @Override
    public void open(OpenContext openContext) {
        windows = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("windows", Long.class, WinAcc.class));
        warmingTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("warmingTs", Long.class));
        flap = getRuntimeContext().getState(
                new ValueStateDescriptor<>("flap", FlapState.class));
        overcurrent = getRuntimeContext().getState(
                new ValueStateDescriptor<>("overcurrent", OcState.class));
    }

    @Override
    public void processElement(GridTelemetry t, Context ctx, Collector<ComponentAggregate> out) throws Exception {
        long ts = t.getTimestamp().toEpochMilli();
        aggregate(t, ts, ctx);
        switch (t.getComponentType()) {
            case "TRANSFORMER" -> detectOverheat(t, ts, ctx);
            case "BREAKER" -> detectFlapping(t, ts, ctx);
            case "FEEDER" -> detectOvercurrent(t, ts, ctx);
        }
    }

    private void aggregate(GridTelemetry t, long ts, Context ctx) throws Exception {
        long winStart = ts - Math.floorMod(ts, WINDOW_MS);
        long winEnd = winStart + WINDOW_MS;
        if (winEnd - 1 <= ctx.timerService().currentWatermark()) {
            return;
        }
        WinAcc acc = windows.get(winStart);
        if (acc == null) {
            acc = new WinAcc();
            ctx.timerService().registerEventTimeTimer(winEnd - 1);
        }
        acc.count++;
        if (t.getVoltage() != null) {
            acc.voltageCount++;
            acc.voltageSum += t.getVoltage();
            acc.voltageMin = Math.min(acc.voltageMin, t.getVoltage());
            acc.voltageMax = Math.max(acc.voltageMax, t.getVoltage());
        }
        if (t.getCurrent() != null) {
            acc.currentCount++;
            acc.currentSum += t.getCurrent();
        }
        if (t.getOilTemp() != null) {
            acc.hasOilTemp = true;
            acc.maxOilTemp = Math.max(acc.maxOilTemp, t.getOilTemp());
        }
        windows.put(winStart, acc);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ComponentAggregate> out) throws Exception {
        long winStart = timestamp + 1 - WINDOW_MS;
        WinAcc acc = windows.get(winStart);
        if (acc == null) {
            return;
        }
        windows.remove(winStart);
        out.collect(new ComponentAggregate(
                ctx.getCurrentKey(),
                acc.count,
                round2(acc.voltageCount > 0 ? acc.voltageSum / acc.voltageCount : 0),
                round2(acc.voltageCount > 0 ? acc.voltageMin : 0),
                round2(acc.voltageCount > 0 ? acc.voltageMax : 0),
                round2(acc.currentCount > 0 ? acc.currentSum / acc.currentCount : 0),
                acc.hasOilTemp ? round2(acc.maxOilTemp) : null,
                Instant.ofEpochMilli(winStart),
                Instant.ofEpochMilli(winStart + WINDOW_MS)
        ));
    }

    private void detectOverheat(GridTelemetry t, long ts, Context ctx) throws Exception {
        Double oil = t.getOilTemp();
        if (oil == null) return;
        if (oil > 105.0) {
            Long warming = warmingTs.value();
            if (warming != null && ts - warming <= OVERHEAT_WINDOW_MS) {
                ctx.output(ALERTS, new GridAlert(
                        t.getComponentId(), t.getSubstationId(), "TRANSFORMER_OVERHEAT",
                        String.format("Transformer %s oil temp exceeded 105 C (%.1f C)",
                                t.getComponentId(), oil),
                        t.getTimestamp()));
                warmingTs.clear();
            }
        } else if (oil > 90.0) {
            warmingTs.update(ts);
        }
    }

    private void detectFlapping(GridTelemetry t, long ts, Context ctx) throws Exception {
        String status = t.getBreakerStatus();
        if (status == null) return;
        boolean open = "OPEN".equals(status);
        FlapState f = flap.value();
        if (f == null) f = new FlapState();
        if (f.stage > 0 && ts - f.firstTs > FLAPPING_WINDOW_MS) {
            f.stage = 0;
        }
        switch (f.stage) {
            case 0 -> {
                if (open) {
                    f.stage = 1;
                    f.firstTs = ts;
                }
            }
            case 1 -> { if (!open) f.stage = 2; }
            case 2 -> { if (open) f.stage = 3; }
            case 3 -> {
                if (!open) {
                    ctx.output(ALERTS, new GridAlert(
                            t.getComponentId(), t.getSubstationId(), "BREAKER_FLAPPING",
                            String.format("Breaker %s flapped OPEN->CLOSED->OPEN->CLOSED within window",
                                    t.getComponentId()),
                            t.getTimestamp()));
                    f.stage = 0;
                }
            }
        }
        flap.update(f);
    }

    private void detectOvercurrent(GridTelemetry t, long ts, Context ctx) throws Exception {
        Double current = t.getCurrent();
        if (current == null) return;
        OcState o = overcurrent.value();
        if (current > 1.2 * RATED_CURRENT) {
            if (o == null) o = new OcState();
            if (o.count == 0 || ts - o.firstTs > OVERCURRENT_WINDOW_MS) {
                o.count = 1;
                o.firstTs = ts;
                o.maxCurrent = current;
            } else {
                o.count++;
                o.maxCurrent = Math.max(o.maxCurrent, current);
            }
            if (o.count >= 5) {
                ctx.output(ALERTS, new GridAlert(
                        t.getComponentId(), t.getSubstationId(), "SUSTAINED_OVERCURRENT",
                        String.format("Feeder %s sustained overcurrent: %d readings above 480 A, max %.1f A",
                                t.getComponentId(), o.count, o.maxCurrent),
                        t.getTimestamp()));
                o.count = 0;
            }
            overcurrent.update(o);
        } else if (o != null && o.count > 0) {
            // pattern requires strictly consecutive overcurrent readings
            overcurrent.clear();
        }
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
