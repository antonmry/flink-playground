package galiglobal.flink.eventTime;

import java.io.Serializable;

public class SensorData implements Serializable {

    private static final long serialVersionUID = 3065800674328381716L;
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

    @Override
    public String toString() {
        return "SensorData{" +
            "id='" + id + '\'' +
            ", timestamp=" + timestamp +
            ", measure=" + measure +
            '}';
    }
}
