package galiglobal.flink.eventTime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class BoundedOutOfOrdernessStrategyJob {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedOutOfOrdernessStrategyJob.class);

    private SourceFunction<SensorData> source;
    private SinkFunction<SensorData> sink;

    public BoundedOutOfOrdernessStrategyJob(SourceFunction<SensorData> source, SinkFunction<SensorData> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {

        Properties props = new Properties();
        props.put("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory");
        Configuration conf = ConfigurationUtils.createConfiguration(props);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // https://issues.apache.org/jira/browse/FLINK-19317
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(Duration.ofMillis(100).toMillis());

        LOG.debug("Start Flink example job");

        DataStream<SensorData> sensorStream =
            env.addSource(source)
                .returns(TypeInformation.of(SensorData.class));

        var sensorEventTimeStream =
            sensorStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorData>forBoundedOutOfOrderness(
                    Duration.ofMillis(100)
                ).withTimestampAssigner(
                    (event, timestamp) -> event.getTimestamp()
                )
            );

        sensorEventTimeStream
            .transform("debugFilter", sensorEventTimeStream.getType(), new StreamWatermarkDebugFilter<>())
            .keyBy((event) -> event.getId())
            .process(new TimeoutFunction())
            .addSink(sink);

        LOG.debug("Stop Flink example job");
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        BoundedOutOfOrdernessStrategyJob job = new BoundedOutOfOrdernessStrategyJob(new RandomSensorSource(), new PrintSinkFunction<>());
        job.execute();
    }

}