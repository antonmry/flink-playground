package galiglobal.flink.eventTime;

public class SensorData {

    private final String id;
    private final Long timestamp;
    private final Double measure;

    public SensorData(String id, Long timestamp, Double measure) {
        this.id = id;
        this.timestamp = timestamp;
        this.measure = measure;
    }

    public String getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Double getMeasure() {
        return measure;
    }
}
