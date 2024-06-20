package com.galiglobal;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Datagen2KafkaStreamingJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(10_000);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		StreamStatementSet statementSet = tableEnv.createStatementSet();

		// create some source
		TableDescriptor sourceDescriptor =
			TableDescriptor.forConnector("datagen")
				//.option("number-of-rows", "3")
				.option("rows-per-second", "1")
				.schema(
					Schema.newBuilder()
						.column("json", DataTypes.STRING())
						.build())
				.build();

		TableDescriptor sinkDescriptor = TableDescriptor.forConnector("print").build();
		Table table = tableEnv.from(sourceDescriptor);
		statementSet.add(table.insertInto(sinkDescriptor));

		statementSet.attachAsDataStream();
		tableEnv.createTemporaryView("InputTable", table);

		// Kafka
		String topic = "test_p1";
		String bootstraps = "xxx:9092";
		String groupId = "flink-paimon";
		final String createTable =
			String.format(
				"CREATE TABLE kafka (\n"
					+ "  `json` STRING\n"
					+ ") WITH (\n"
					+ "  'connector' = 'kafka',\n"
					//+ "  'connector' = 'upsert-kafka',\n"
					+ "  'topic' = '%s',\n"
					+ "  'properties.bootstrap.servers' = '%s',\n"
					+ "  'properties.group.id' = '%s',\n"
					//+ "  'key.format' = 'raw',\n"
					+ "  'value.format' = 'raw'\n"
					+ ")",
				topic, bootstraps, groupId);
		tableEnv.executeSql(createTable);

		// For debugging
		// tableEnv.executeSql("SELECT * FROM InputTable").print();

		// insert into paimon table from your data stream table
		tableEnv.executeSql("INSERT INTO default_catalog.default_database.kafka SELECT json FROM InputTable");

	}
}
