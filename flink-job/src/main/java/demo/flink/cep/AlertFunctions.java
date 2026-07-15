package demo.flink.cep;

import demo.flink.model.GridAlert;
import demo.flink.model.GridTelemetry;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class AlertFunctions {

    private AlertFunctions() {
    }

    public static PatternProcessFunction<GridTelemetry, GridAlert> sagTripProcessor() {
        return new PatternProcessFunction<GridTelemetry, GridAlert>() {
            @Override
            public void processMatch(Map<String, List<GridTelemetry>> match, Context ctx, Collector<GridAlert> out) {
                List<GridTelemetry> sagEvents = match.get("sag");
                List<GridTelemetry> tripEvents = match.get("trip");
                if (sagEvents == null || sagEvents.isEmpty() || tripEvents == null || tripEvents.isEmpty()) return;

                GridTelemetry sag = sagEvents.get(0);
                GridTelemetry trip = tripEvents.get(0);
                out.collect(new GridAlert(
                        trip.getComponentId(),
                        sag.getSubstationId(),
                        "VOLTAGE_SAG_BREAKER_TRIP",
                        String.format("Feeder %s sagged %.2f kV then breaker %s tripped OPEN",
                                sag.getComponentId(), sag.getVoltage(), trip.getComponentId()),
                        trip.getTimestamp()
                ));
            }
        };
    }

    public static PatternProcessFunction<GridTelemetry, GridAlert> overheatProcessor() {
        return new PatternProcessFunction<GridTelemetry, GridAlert>() {
            @Override
            public void processMatch(Map<String, List<GridTelemetry>> match, Context ctx, Collector<GridAlert> out) {
                List<GridTelemetry> warming = match.get("warming");
                List<GridTelemetry> critical = match.get("critical");
                if (warming == null || warming.isEmpty() || critical == null || critical.isEmpty()) return;

                GridTelemetry crit = critical.get(0);
                out.collect(new GridAlert(
                        crit.getComponentId(),
                        crit.getSubstationId(),
                        "TRANSFORMER_OVERHEAT",
                        String.format("Transformer %s oil temp exceeded 105 C (%.1f C)",
                                crit.getComponentId(), crit.getOilTemp()),
                        crit.getTimestamp()
                ));
            }
        };
    }

    public static PatternProcessFunction<GridTelemetry, GridAlert> flappingProcessor() {
        return new PatternProcessFunction<GridTelemetry, GridAlert>() {
            @Override
            public void processMatch(Map<String, List<GridTelemetry>> match, Context ctx, Collector<GridAlert> out) {
                List<GridTelemetry> closed2 = match.get("closed2");
                if (closed2 == null || closed2.isEmpty()) return;

                GridTelemetry last = closed2.get(0);
                out.collect(new GridAlert(
                        last.getComponentId(),
                        last.getSubstationId(),
                        "BREAKER_FLAPPING",
                        String.format("Breaker %s flapped OPEN->CLOSED->OPEN->CLOSED within window",
                                last.getComponentId()),
                        last.getTimestamp()
                ));
            }
        };
    }

    public static PatternProcessFunction<GridTelemetry, GridAlert> overcurrentProcessor() {
        return new PatternProcessFunction<GridTelemetry, GridAlert>() {
            @Override
            public void processMatch(Map<String, List<GridTelemetry>> match, Context ctx, Collector<GridAlert> out) {
                List<GridTelemetry> over = match.get("over");
                if (over == null || over.isEmpty()) return;

                GridTelemetry last = over.get(over.size() - 1);
                double maxCurrent = 0;
                for (GridTelemetry t : over) {
                    if (t.getCurrent() != null && t.getCurrent() > maxCurrent) maxCurrent = t.getCurrent();
                }
                out.collect(new GridAlert(
                        last.getComponentId(),
                        last.getSubstationId(),
                        "SUSTAINED_OVERCURRENT",
                        String.format("Feeder %s sustained overcurrent: %d readings above 480 A, max %.1f A",
                                last.getComponentId(), over.size(), maxCurrent),
                        last.getTimestamp()
                ));
            }
        };
    }
}
