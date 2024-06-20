package galiglobal.flink.eventTime;

public class SensorDataState {
    private SensorData prevMsg;
    private long timerSetFor;

    public SensorData getPrevMsg() {
        return prevMsg;
    }

    public void setPrevMsg(SensorData prevMsg) {
        this.prevMsg = prevMsg;
    }

    public long getTimerSetFor() {
        return timerSetFor;
    }

    public void setTimerSetFor(long timerSetFor) {
        this.timerSetFor = timerSetFor;
    }
}
