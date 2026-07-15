package demo.flink.cep;

import demo.flink.model.GridTelemetry;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public final class FaultPatterns {

    private static final double NOMINAL_VOLTAGE_34_5 = 34.5;
    private static final double NOMINAL_VOLTAGE_13_8 = 13.8;
    private static final double RATED_CURRENT = 400.0;

    private FaultPatterns() {
    }

    public static Pattern<GridTelemetry, ?> voltageSagBreakerTrip() {
        return Pattern.<GridTelemetry>begin("sag", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        if (!"FEEDER".equals(t.getComponentType()) || t.getVoltage() == null) return false;
                        String sub = t.getSubstationId();
                        double nominal = "SUB-B".equals(sub) ? NOMINAL_VOLTAGE_13_8 : NOMINAL_VOLTAGE_34_5;
                        return t.getVoltage() < 0.90 * nominal;
                    }
                })
                .times(3)
                .followedBy("trip")
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        return "BREAKER".equals(t.getComponentType()) && "OPEN".equals(t.getBreakerStatus());
                    }
                })
                .within(java.time.Duration.ofSeconds(30));
    }

    public static Pattern<GridTelemetry, ?> transformerOverheat() {
        return Pattern.<GridTelemetry>begin("warming", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        // upper bound stops re-matching on already-critical readings during cool-down
                        return "TRANSFORMER".equals(t.getComponentType())
                                && t.getOilTemp() != null && t.getOilTemp() > 90.0 && t.getOilTemp() <= 105.0;
                    }
                })
                .followedBy("critical")
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        return "TRANSFORMER".equals(t.getComponentType())
                                && t.getOilTemp() != null && t.getOilTemp() > 105.0;
                    }
                })
                .within(java.time.Duration.ofSeconds(90));
    }

    public static Pattern<GridTelemetry, ?> breakerFlapping() {
        return Pattern.<GridTelemetry>begin("open1", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        return "BREAKER".equals(t.getComponentType()) && "OPEN".equals(t.getBreakerStatus());
                    }
                })
                .followedBy("closed1")
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        return "BREAKER".equals(t.getComponentType()) && "CLOSED".equals(t.getBreakerStatus());
                    }
                })
                .followedBy("open2")
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        return "BREAKER".equals(t.getComponentType()) && "OPEN".equals(t.getBreakerStatus());
                    }
                })
                .followedBy("closed2")
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        return "BREAKER".equals(t.getComponentType()) && "CLOSED".equals(t.getBreakerStatus());
                    }
                })
                .within(java.time.Duration.ofSeconds(30));
    }

    public static Pattern<GridTelemetry, ?> sustainedOvercurrent() {
        return Pattern.<GridTelemetry>begin("over", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<GridTelemetry>() {
                    @Override
                    public boolean filter(GridTelemetry t) {
                        return "FEEDER".equals(t.getComponentType())
                                && t.getCurrent() != null && t.getCurrent() > 1.2 * RATED_CURRENT;
                    }
                })
                .times(5)
                .consecutive()
                .within(java.time.Duration.ofSeconds(20));
    }
}
