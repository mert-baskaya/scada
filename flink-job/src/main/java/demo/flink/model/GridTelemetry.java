package demo.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class GridTelemetry {

    @JsonProperty("componentId")
    private String componentId;

    @JsonProperty("componentType")
    private String componentType;

    @JsonProperty("substationId")
    private String substationId;

    @JsonProperty("voltage")
    private Double voltage;

    @JsonProperty("current")
    private Double current;

    @JsonProperty("frequency")
    private Double frequency;

    @JsonProperty("breakerStatus")
    private String breakerStatus;

    @JsonProperty("oilTemp")
    private Double oilTemp;

    @JsonProperty("tapPosition")
    private Integer tapPosition;

    @JsonProperty("timestamp")
    private Instant timestamp;

    public GridTelemetry() {
    }

    public GridTelemetry(String componentId, String componentType, String substationId,
                         Double voltage, Double current, Double frequency,
                         String breakerStatus, Double oilTemp, Integer tapPosition,
                         Instant timestamp) {
        this.componentId = componentId;
        this.componentType = componentType;
        this.substationId = substationId;
        this.voltage = voltage;
        this.current = current;
        this.frequency = frequency;
        this.breakerStatus = breakerStatus;
        this.oilTemp = oilTemp;
        this.tapPosition = tapPosition;
        this.timestamp = timestamp;
    }

    public String getComponentId() { return componentId; }
    public void setComponentId(String componentId) { this.componentId = componentId; }

    public String getComponentType() { return componentType; }
    public void setComponentType(String componentType) { this.componentType = componentType; }

    public String getSubstationId() { return substationId; }
    public void setSubstationId(String substationId) { this.substationId = substationId; }

    public Double getVoltage() { return voltage; }
    public void setVoltage(Double voltage) { this.voltage = voltage; }

    public Double getCurrent() { return current; }
    public void setCurrent(Double current) { this.current = current; }

    public Double getFrequency() { return frequency; }
    public void setFrequency(Double frequency) { this.frequency = frequency; }

    public String getBreakerStatus() { return breakerStatus; }
    public void setBreakerStatus(String breakerStatus) { this.breakerStatus = breakerStatus; }

    public Double getOilTemp() { return oilTemp; }
    public void setOilTemp(Double oilTemp) { this.oilTemp = oilTemp; }

    public Integer getTapPosition() { return tapPosition; }
    public void setTapPosition(Integer tapPosition) { this.tapPosition = tapPosition; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "GridTelemetry{componentId='" + componentId + "', type=" + componentType + ", voltage=" + voltage + "}";
    }
}
