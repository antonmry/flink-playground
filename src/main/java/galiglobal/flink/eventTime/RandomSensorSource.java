package galiglobal.flink.eventTime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class RandomSensorSource extends RichParallelSourceFunction<SensorData> {

	private volatile boolean cancelled = false;
	private Random random;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		random = new Random();
	}

	@Override
	public void run(SourceContext<SensorData> ctx) throws Exception {
		while (!cancelled) {
			Double nextDouble = random.nextDouble();
			synchronized (ctx.getCheckpointLock()) {
				ctx.collect(new SensorData("0", 0L, nextDouble));
			}
		}
	}

	@Override
	public void cancel() {
		cancelled = true;
	}
}
