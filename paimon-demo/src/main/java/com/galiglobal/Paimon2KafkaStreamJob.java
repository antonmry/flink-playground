package com.galiglobal;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Paimon2KafkaStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1_000);

		// create environments of both APIs
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// Kafka
		String topic = "test_p1";
		String bootstraps = "xxx:9092";
		String groupId = "flink-paimon";
		final String createTable =
			String.format(
				"CREATE TABLE kafka (\n"
					+ "  `id` BIGINT,\n"
					+ "  `json` STRING,\n"
					+ "  PRIMARY KEY (id) NOT ENFORCED\n"
					+ ") WITH (\n"
					//+ "  'connector' = 'kafka',\n"
					+ "  'connector' = 'upsert-kafka',\n"
					+ "  'topic' = '%s',\n"
					+ "  'properties.bootstrap.servers' = '%s',\n"
					+ "  'properties.group.id' = '%s',\n"
					+ "  'key.format' = 'raw',\n"
					+ "  'value.format' = 'json'\n"
					+ ")",
				topic, bootstraps, groupId);
		tableEnv.executeSql(createTable);

		// Paimon
		tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = 's3://arodriguez-flink-poc/paimon', 's3.access-key' = 'xxx', 's3.secret-key' = 'xxx')");

		tableEnv.executeSql("USE CATALOG paimon");

		Table table = tableEnv.sqlQuery("SELECT 1 as id, json FROM sink_paimon_table");
		tableEnv.createTemporaryView("InputTable", table);

		// Pipeline
		//tableEnv.from("InputTable").insertInto("default_catalog.default_database.kafka").execute();
		tableEnv.executeSql("INSERT INTO default_catalog.default_database.kafka SELECT id, json FROM InputTable");
	}
}
