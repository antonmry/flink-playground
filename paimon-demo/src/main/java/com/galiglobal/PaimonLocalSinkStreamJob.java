package com.galiglobal;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class PaimonLocalSinkStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// create a changelog DataStream
		DataStream<Row> dataStream =
			env.fromElements(
					Row.ofKind(RowKind.INSERT, "Alice"),
					Row.ofKind(RowKind.INSERT, "Bob"))
				.returns(
					Types.ROW_NAMED(
						new String[] {"json"},
						Types.STRING));

		// interpret the DataStream as a Table
		Schema schema = Schema.newBuilder()
			.column("json", DataTypes.STRING())
			.build();
		Table table = tableEnv.fromChangelogStream(dataStream, schema);

		// create paimon catalog
		tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file:/tmp/paimon')");
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

	}
}
