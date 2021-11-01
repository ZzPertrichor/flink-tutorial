package org.example.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.apitest.bean.SensorReading;

import java.util.Arrays;

/**
 * @author zm
 * @since 2021-10-20
 */
public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );
        final DataStream<Integer> integerDataStream = env.fromElements(1, 3, 4, 5, 23, 132);
        dataStream.print("data");
        integerDataStream.print("int");
        env.execute("Test Collection");
    }
}
