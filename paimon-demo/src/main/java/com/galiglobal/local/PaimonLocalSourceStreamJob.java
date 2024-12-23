package com.galiglobal.local;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PaimonLocalSourceStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file:/tmp/paimon')");

		tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS  paimon.paimon");

		tableEnv.executeSql("""
			CREATE TABLE IF NOT EXISTS paimon.paimon.PaimonTest (
			    random_text STRING,
			    event_time   TIMESTAMP_LTZ(3)
			) WITH (
			  'connector' = 'paimon'
			)
			""");

		Table table = tableEnv.sqlQuery("""
		SELECT TIMESTAMPDIFF(SECOND, event_time, CURRENT_TIMESTAMP) as time_diff 
		 FROM paimon.paimon.PaimonTest //*+ OPTIONS('scan.mode' = 'latest' ) *//"
 		""");

		DataStream<Row> dataStream = tableEnv.toChangelogStream(table);
		dataStream.executeAndCollect().forEachRemaining(System.out::println);

		env.execute("Local paimon source stream job");
	}
}
