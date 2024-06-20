package galiglobal.flink.utils;

import galiglobal.flink.eventTime.SensorData;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SinkCollectingSensorData implements SinkFunction<SensorData> {

	public static final List<SensorData> result =
			Collections.synchronizedList(new ArrayList<>());

	public void invoke(SensorData value, Context context) throws Exception {
		result.add(value);
	}
}
