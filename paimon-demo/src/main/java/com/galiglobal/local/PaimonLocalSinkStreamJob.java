package com.galiglobal.local;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonLocalSinkStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5_000);
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

		tableEnv.executeSql("""
			CREATE TABLE DatagenTest 
			WITH (
			  'connector' = 'datagen'
			) LIKE paimon.paimon.PaimonTest (EXCLUDING ALL)
			""");

		tableEnv.executeSql("INSERT INTO paimon.paimon.PaimonTest SELECT * FROM DatagenTest");

		env.execute("Local paimon sink stream job");
	}
}
