package org.example.apitest.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 * @since 2021-10-22
 */
public class TransformTest5_RichFunction {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        final DataStream<SensorReading> input = env
                .readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .map(SensorReading::fromText);

        input.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(SensorReading value) {
                        return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化工作，一般是定义状态，或者建立数据库连接
                        System.out.println("open");
                    }

                    @Override
                    public void close() {
                        // 一般是关闭连接和清空状态的守卫操作
                        System.out.println("close");
                    }
                })
                .print();

        env.execute();
    }
}
