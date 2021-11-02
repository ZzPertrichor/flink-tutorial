package org.example.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zm
 */
public class TableTest2_CommonApi {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .column("temp", DataTypes.DOUBLE())
                .build();
        tEnv.createTemporaryTable("sensor", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option("path", "C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .format("csv")
                .build());

        // Query Transformation
        final Table sensor = tEnv.from("sensor");
        // 1. Table API
        // Simple
        final Table table1 = sensor.select($("id"), $("temp"))
                .filter($("id").isEqual("sensor_6"));
        // Aggregate
        final Table table2 = sensor.groupBy($("id"))
                .select(
                        $("id"),
                        $("id").count().as("count"),
                        $("temp").avg().as("avgTemp")
                );
        // 2. SQL
        final Table table3 = tEnv.sqlQuery("select id, temp from sensor where id = 'sensor_6'");
        final Table table4 = tEnv.sqlQuery("select id, count(id) as `count`, avg(temp) as avgTemp from sensor group by id");

        tEnv.toDataStream(table1).print("table1");
        tEnv.toChangelogStream(table2).print("table2");
        tEnv.toDataStream(table3).print("table3");
        tEnv.toChangelogStream(table4).print("table4");

        env.execute();
    }
}
