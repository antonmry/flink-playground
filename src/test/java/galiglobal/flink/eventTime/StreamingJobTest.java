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
    public void testCompletePipeline() throws Exception {

        ParallelSourceFunction<SensorData> source =
            new ParallelCollectionSource(Arrays.asList(new SensorData("sensor0", 0L, 0.1)));
        SinkCollectingSensorData sink = new SinkCollectingSensorData();
        StreamingJob job = new StreamingJob(source, sink);

        job.execute();

        sink.result.stream().forEach(s -> System.out.println(s));
        // assertThat(sink.result).containsExactlyInAnyOrder(2L, 11L, -9L);
    }
}
