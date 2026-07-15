package demo.flink;

public class AggregateAccumulator {
    int count;
    int voltageCount;
    double voltageSum;
    double voltageMin;
    double voltageMax;
    int currentCount;
    double currentSum;
    Double maxOilTemp;

    AggregateAccumulator() {
        this.count = 0;
        this.voltageCount = 0;
        this.voltageSum = 0.0;
        this.voltageMin = Double.MAX_VALUE;
        this.voltageMax = -Double.MAX_VALUE;
        this.currentCount = 0;
        this.currentSum = 0.0;
        this.maxOilTemp = null;
    }
}
