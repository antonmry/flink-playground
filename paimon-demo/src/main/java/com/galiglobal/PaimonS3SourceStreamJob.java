package com.galiglobal;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PaimonS3SourceStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// create environments of both APIs
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = 's3://xxx/paimon', 's3.access-key' = 'xxx', 's3.secret-key' = 'xxx')");

		tableEnv.executeSql("USE CATALOG paimon");

		Table table = tableEnv.sqlQuery("SELECT * FROM sink_paimon_table");
		DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

		// use this datastream
		dataStream.executeAndCollect().forEachRemaining(System.out::println);

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
