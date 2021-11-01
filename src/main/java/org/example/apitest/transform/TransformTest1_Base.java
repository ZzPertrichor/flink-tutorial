package org.example.apitest.transform;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author zm
 * @since 2021-10-22
 */
public class TransformTest1_Base {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<String> input = env.readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt");

        input.map(String::length)
                .print("map");
        input.flatMap((String value, Collector<String> out) ->
                        Arrays.stream(value.split(",")).forEach(out::collect))
                .returns(TypeInformation.of(new TypeHint<String>() {}))
                .print("flat-map");
        input.filter(value -> value.startsWith("sensor_1"))
                .print("filter");

        env.execute();
    }
}
