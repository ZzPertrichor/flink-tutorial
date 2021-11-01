package org.example.apitest.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 * @since 2021-10-22
 */
public class TransformTest4_MultipleStream {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<String> input = env
                .readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt");

        final OutputTag<SensorReading> highTag = new OutputTag<SensorReading>("high") {};
        final OutputTag<SensorReading> lowTag = new OutputTag<SensorReading>("low") {};
        final SingleOutputStreamOperator<SensorReading> output = input
                .map(SensorReading::fromText)
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value,
                                               ProcessFunction<SensorReading, SensorReading>.Context ctx,
                                               Collector<SensorReading> out) {
                        out.collect(value);
                        if (value.getTemperature() > 30) {
                            ctx.output(highTag, value);
                        } else {
                            ctx.output(lowTag, value);
                        }
                    }
                });
        final DataStream<SensorReading> highOut = output.getSideOutput(highTag);
        final DataStream<SensorReading> lowOut = output.getSideOutput(lowTag);

        highOut.print("high");
        lowOut.print("low");
        output.print("all");

        // 合流，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        highOut.map(SensorReading::toTuple)
                .connect(lowOut)
                .map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
                    @Override
                    public Object map1(Tuple2<String, Double> value) {
                        return new Tuple3<>(value.f0, value.f1, "high temp warning");
                    }

                    @Override
                    public Object map2(SensorReading value) {
                        return new Tuple2<>(value.getId(), "normal");
                    }
                })
                .print();

        // union联合多条流
        highOut.union(lowOut, output)
                .print();

        env.execute();
    }
}
