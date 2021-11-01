package org.example.apitest.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 */
public class ProcessTest1_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .socketTextStream("localhost", 8888)
                .map(SensorReading::fromText);

        source.keyBy(SensorReading::getId)
                .process(new MyProcess())
                .print();

        env.execute();
    }

    private static class MyProcess extends KeyedProcessFunction<String, SensorReading, Integer> {

        private ValueState<Long> timer;

        @Override
        public void open(Configuration parameters) {
            timer = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value,
                                   KeyedProcessFunction<String, SensorReading, Integer>.Context ctx,
                                   Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // context
            ctx.timestamp();
            ctx.getCurrentKey();
            ctx.output(new OutputTag<>(""), "");
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + 1000L);
            timer.update(ctx.timestamp() + 1000L);
            // ctx.timerService().deleteProcessingTimeTimer(timer.value());
            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<String, SensorReading, Integer>.OnTimerContext ctx,
                            Collector<Integer> out) {
            System.out.println(timestamp + " timer triggered.");
            ctx.getCurrentKey();
            // ctx.output();
            ctx.timeDomain();
        }
    }
}
