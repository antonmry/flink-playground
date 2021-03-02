package galiglobal.flink.eventTime;

import galiglobal.flink.utils.ParallelCollectionSource;
import galiglobal.flink.utils.SinkCollectingSensorData;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StreamingJobTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());

    @Test
    public void testBoundedOutOfOrdernessStrategyJob() throws Exception {

        var source = new ParallelCollectionSource(generateSensorData());
        var sink = new SinkCollectingSensorData();
        var job = new BoundedOutOfOrdernessStrategyJob(source, sink);
        job.execute();

        sink.result.stream().forEach(s -> System.out.println(s));
        // TODO
        // assertThat(sink.result).containsExactlyInAnyOrder(2L, 11L, -9L);
    }

    @Test
    public void testCustomStrategyJob() throws Exception {

        var source = new ParallelCollectionSource(generateSensorData());
        var sink = new SinkCollectingSensorData();
        var job = new CustomStrategyJob(source, sink);
        job.execute();

        sink.result.stream().forEach(s -> System.out.println(s));
        // TODO
        // assertThat(sink.result).containsExactlyInAnyOrder(2L, 11L, -9L);
    }

    private List<SensorData> generateSensorData() {
        return Arrays.asList(
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
        );
    }
}
