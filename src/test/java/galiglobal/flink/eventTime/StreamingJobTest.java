package galiglobal.flink.eventTime;

import galiglobal.flink.IncrementMapFunction;
import galiglobal.flink.utils.ParallelCollectionSource;
import galiglobal.flink.utils.SinkCollectingSensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;

public class StreamingJobTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());

    @Test
    public void test_Fail_Pipeline() throws Exception {

        var source = new ParallelCollectionSource(
            Arrays.asList(
                new SensorData("sensor0", 0L, 0.1),
                new SensorData("sensor1", 0L, 0.2),
                new SensorData("sensor0", 100L, 0.3),
                new SensorData("sensor1", 100L, 0.4),
                new SensorData("sensor0", 200L, 0.5),
                //new SensorData("sensor1", 200L, 0.6),
                new SensorData("sensor0", 300L, 0.7),
                new SensorData("sensor1", 300L, 0.8),
                new SensorData("sensor0", 400L, 0.9),
                new SensorData("sensor1", 400L, 1.0)
            )
        );

        var sink = new SinkCollectingSensorData();
        var job = new InternalFailStreamingJob(source, sink);

        job.execute();

        sink.result.stream().forEach(s -> System.out.println(s));
        // assertThat(sink.result).containsExactlyInAnyOrder(2L, 11L, -9L);
    }

    @Test
    public void testCompletePipeline() throws Exception {

        var source = new ParallelCollectionSource(
            Arrays.asList(
                new SensorData("sensor0", 0L, 0.1),
                new SensorData("sensor1", 0L, 0.2),
                new SensorData("sensor0", 100L, 0.3),
                new SensorData("sensor1", 100L, 0.4),
                new SensorData("sensor0", 200L, 0.5),
                //new SensorData("sensor1", 200L, 0.6),
                new SensorData("sensor0", 300L, 0.7),
                new SensorData("sensor1", 300L, 0.8),
                new SensorData("sensor0", 400L, 0.9),
                new SensorData("sensor1", 400L, 1.0)
            )
        );

        var sink = new SinkCollectingSensorData();
        var job = new InternalStreamingJob(source, sink);

        job.execute();

        sink.result.stream().forEach(s -> System.out.println(s));
        // assertThat(sink.result).containsExactlyInAnyOrder(2L, 11L, -9L);
    }
}
