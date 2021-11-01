package org.example.apitest.process;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 */
public class ProcessTest3_SideOutputCase {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .socketTextStream("localhost", 8888)
                .map(SensorReading::fromText);

        final OutputTag<SensorReading> low = new OutputTag<SensorReading>("low-temp") {};
        final SingleOutputStreamOperator<SensorReading> highOut = source
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value,
                                               ProcessFunction<SensorReading, SensorReading>.Context ctx,
                                               Collector<SensorReading> out) {
                        if (value.getTemperature() > 30) {
                            // high temperature
                            out.collect(value);
                        } else {
                            // low temperature
                            ctx.output(low, value);
                        }
                    }
                });

        highOut.print("high-temp");
        highOut.getSideOutput(low).print("low-temp");

        env.execute();
    }
}
