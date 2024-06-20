package galiglobal.flink.eventTime;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeoutFunction
    extends KeyedProcessFunction<String, SensorData, SensorData> {

    static final long TIMEOUT_MS = 100;
    private static final Logger LOG = LoggerFactory.getLogger(TimeoutFunction.class);

    private ValueState<SensorDataState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("DPProcessState", SensorDataState.class));
    }

    @Override
    public void processElement(
        SensorData in,
        Context ctx,
        Collector<SensorData> out) throws Exception {

        SensorDataState processState = state.value();
        if (processState == null) {
            processState = new SensorDataState();
        }

        // Cancel previous timer
        if (processState.getTimerSetFor() != 0) {
                ctx.timerService().deleteEventTimeTimer(processState.getTimerSetFor());
        }

        // Schedule a timeout
        long trigTime = in.getTimestamp() + TIMEOUT_MS;
        ctx.timerService().registerEventTimeTimer(trigTime);
        out.collect(in);

        // write the state back
        processState.setTimerSetFor(trigTime);
        processState.setPrevMsg(in);
        state.update(processState);
    }

    @Override
    public void onTimer(
        long timestamp,
        OnTimerContext ctx,
        Collector<SensorData> out) throws Exception {

        SensorDataState processState = state.value();
        out.collect(new SensorData(
            processState.getPrevMsg().getId(),
            timestamp,
            -1.0
        ));

        System.out.println("Timer: " + timestamp + " -> " + processState.getPrevMsg().getId());
    }
}
