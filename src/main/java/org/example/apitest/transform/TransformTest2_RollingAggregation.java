package org.example.apitest.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 * @since 2021-10-22
 */
public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        env.readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .map(SensorReading::fromText)
                .keyBy(SensorReading::getId)
                // 滚动聚合，取当前最大温度值
                //.max("temperature")
                // 取最大温度读数
                .maxBy("temperature")
                .print();
        env.execute();
    }
}
