package demo.flink.model;

import java.time.Instant;

public class GridAlert {

    private String componentId;
    private String substationId;
    private String alertType;
    private String message;
    private Instant timestamp;

    public GridAlert() {
    }

    public GridAlert(String componentId, String substationId, String alertType, String message, Instant timestamp) {
        this.componentId = componentId;
        this.substationId = substationId;
        this.alertType = alertType;
        this.message = message;
        this.timestamp = timestamp;
    }

    public String getComponentId() { return componentId; }
    public void setComponentId(String componentId) { this.componentId = componentId; }

    public String getSubstationId() { return substationId; }
    public void setSubstationId(String substationId) { this.substationId = substationId; }

    public String getAlertType() { return alertType; }
    public void setAlertType(String alertType) { this.alertType = alertType; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
