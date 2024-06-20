package com.galiglobal;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonS3SinkStreamingJob {

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

		// create paimon catalog
		tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = 's3://xxx/paimon', 's3.access-key' = 'xxx', 's3.secret-key' = 'xxx')");
		tableEnv.executeSql("USE CATALOG paimon");

		// register the table under a name and perform an aggregation
		tableEnv.createTemporaryView("InputTable", table);

		tableEnv.executeSql("CREATE TABLE IF NOT EXISTS sink_paimon_table (\n" +
			"    json STRING PRIMARY KEY NOT ENFORCED\n" +
			");");

		// For debugging
		// tableEnv.executeSql("SELECT * FROM InputTable").print();

		// insert into paimon table from your data stream table
		tableEnv.executeSql("INSERT INTO sink_paimon_table SELECT * FROM InputTable");

		env.execute();

	}
}
