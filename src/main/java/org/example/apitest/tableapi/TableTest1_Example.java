package org.example.apitest.tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.apitest.bean.SensorReading;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zm
 */
public class TableTest1_Example {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .map(SensorReading::fromText);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final Table table = tableEnv.fromDataStream(source);

        // table api
        final Table result = table.select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_1"));

        // execute SQL
        tableEnv.createTemporaryView("sensor", table);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        final Table sqlResult = tableEnv.sqlQuery(sql);

        // to data stream
        tableEnv.toDataStream(result).print("table-api");
        tableEnv.toDataStream(sqlResult).print("sql");

        env.execute();
    }
}
