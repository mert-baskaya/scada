package demo.flink.hybrid;

import demo.flink.model.GridTelemetry;

/**
 * Pre-filters that shrink the full telemetry stream to the rare events each CEP
 * pattern can match on, so CEP buffers hundreds of events/s instead of the full
 * stream. Safe for the relaxed-contiguity patterns in FaultPatterns; NOT safe for
 * consecutive() patterns, which is why overcurrent uses OvercurrentDetector instead.
 */
public final class PreFilters {

    private static final double NOMINAL_VOLTAGE_34_5 = 34.5;
    private static final double NOMINAL_VOLTAGE_13_8 = 13.8;

    private PreFilters() {
    }

    public static boolean sagTripRelevant(GridTelemetry t) {
        String type = t.getComponentType();
        if ("FEEDER".equals(type) && t.getVoltage() != null) {
            double nominal = "SUB-B".equals(t.getSubstationId()) ? NOMINAL_VOLTAGE_13_8 : NOMINAL_VOLTAGE_34_5;
            return t.getVoltage() < 0.90 * nominal;
        }
        return "BREAKER".equals(type) && "OPEN".equals(t.getBreakerStatus());
    }

    public static boolean overheatRelevant(GridTelemetry t) {
        return "TRANSFORMER".equals(t.getComponentType())
                && t.getOilTemp() != null && t.getOilTemp() > 90.0;
    }

    public static boolean breakerReading(GridTelemetry t) {
        return "BREAKER".equals(t.getComponentType()) && t.getBreakerStatus() != null;
    }

    public static boolean feederReading(GridTelemetry t) {
        return "FEEDER".equals(t.getComponentType());
    }
}
