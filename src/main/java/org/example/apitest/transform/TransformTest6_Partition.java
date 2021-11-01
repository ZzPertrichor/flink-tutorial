package org.example.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 * @since 2021-10-22
 */
public class TransformTest6_Partition {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        final DataStream<SensorReading> input = env
                .readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .map(SensorReading::fromText);

        input.print("input");
        input.shuffle().print("shuffle");
        input.keyBy(SensorReading::getId).print("key by");
        input.global().print("global");

        env.execute();
    }
}
