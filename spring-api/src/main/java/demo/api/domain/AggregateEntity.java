package demo.api.domain;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "aggregates")
public class AggregateEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String componentId;

    private int readingCount;
    private double avgVoltage;
    private double minVoltage;
    private double maxVoltage;
    private double avgCurrent;
    private Double maxOilTemp;

    private Instant windowStart;
    private Instant windowEnd;
    private Instant receivedAt;

    public AggregateEntity() {
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

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

    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }
}
