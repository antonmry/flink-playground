package com.galiglobal;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.state.forst.ForStOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.runtime.state.StateBackendLoader.FORST_STATE_BACKEND_NAME;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env;
        if (args.length > 0 && "test".equals(args[0])) {

            Configuration conf = new Configuration();
            // conf.setString("state.backend.type", "hashmap");
            // conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            conf.set(StateBackendOptions.STATE_BACKEND, FORST_STATE_BACKEND_NAME);
            // conf.set(ForStOptions.REMOTE_DIRECTORY, "s3://arodriguez-flink-poc/flink/db");
            conf.set(ForStOptions.REMOTE_DIRECTORY, "file:///tmp/flink/remote");

            // conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            // conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/flink/checkpoint");
            // conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://arodriguez-flink-poc/flink/checkpoint");
            //  conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5));

            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema =
            Schema.newBuilder()
                .column("record", DataTypes.STRING())
                .columnByMetadata("file.path", DataTypes.STRING())
                .build();

        TableDescriptor tableDescriptor =
            TableDescriptor.forConnector("filesystem")
                .option(FileSystemConnectorOptions.PATH, "file:///tmp/flink/dataset")
                .option(FileSystemConnectorOptions.SOURCE_PATH_REGEX_PATTERN, "/.*/logs_[0-9]+.json")
                .option(FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL, Duration.ofSeconds(5))
                .format("raw")
                .schema(schema)
                .build();

        tEnv.createTable("source_table", tableDescriptor);
        Table table = tEnv.from("source_table");

        DataStream<Row> resultStream = tEnv.toDataStream(table);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = resultStream.flatMap(new Splitter())
            .keyBy(value -> value.f0);

        // Not available yet in 2.0-preview1, only 2.0-SNAPSHOT
        keyedStream.enableAsyncState();

        keyedStream
            //  .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
            .sum(1)
            .print();

        env.execute();
    }

    public static class Splitter implements FlatMapFunction<Row, Tuple2<String, Integer>> {
        @Override
        public void flatMap(Row row, Collector<Tuple2<String, Integer>> out) throws Exception {
            String sentence = row.getField("record").toString();

            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
