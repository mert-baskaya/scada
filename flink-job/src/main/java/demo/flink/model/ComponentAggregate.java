package demo.flink.model;

import java.time.Instant;

public class ComponentAggregate {

    private String componentId;
    private int readingCount;
    private double avgVoltage;
    private double minVoltage;
    private double maxVoltage;
    private double avgCurrent;
    private Double maxOilTemp;
    private Instant windowStart;
    private Instant windowEnd;

    public ComponentAggregate() {
    }

    public ComponentAggregate(String componentId, int readingCount, double avgVoltage,
                              double minVoltage, double maxVoltage, double avgCurrent,
                              Double maxOilTemp, Instant windowStart, Instant windowEnd) {
        this.componentId = componentId;
        this.readingCount = readingCount;
        this.avgVoltage = avgVoltage;
        this.minVoltage = minVoltage;
        this.maxVoltage = maxVoltage;
        this.avgCurrent = avgCurrent;
        this.maxOilTemp = maxOilTemp;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String getComponentId() { return componentId; }
    public void setComponentId(String componentId) { this.componentId = componentId; }

    public int getReadingCount() { return readingCount; }
    public void setReadingCount(int readingCount) { this.readingCount = readingCount; }

    public double getAvgVoltage() { return avgVoltage; }
    public void setAvgVoltage(double avgVoltage) { this.avgVoltage = avgVoltage; }

    public double getMinVoltage() { return minVoltage; }
    public void setMinVoltage(double minVoltage) { this.minVoltage = minVoltage; }

    public double getMaxVoltage() { return maxVoltage; }
    public void setMaxVoltage(double maxVoltage) { this.maxVoltage = maxVoltage; }

    public double getAvgCurrent() { return avgCurrent; }
    public void setAvgCurrent(double avgCurrent) { this.avgCurrent = avgCurrent; }

    public Double getMaxOilTemp() { return maxOilTemp; }
    public void setMaxOilTemp(Double maxOilTemp) { this.maxOilTemp = maxOilTemp; }

    public Instant getWindowStart() { return windowStart; }
    public void setWindowStart(Instant windowStart) { this.windowStart = windowStart; }

    public Instant getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Instant windowEnd) { this.windowEnd = windowEnd; }
}
