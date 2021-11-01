package org.example.apitest.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.apitest.bean.SensorReading;

/**
 * An application case of KeyedProcessFunction.
 *
 * @author zm
 */
public class ProcessTest2_ApplicationCase {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .socketTextStream("localhost", 8888)
                .map(SensorReading::fromText);

        source.keyBy(SensorReading::getId)
                .process(new MonotonousTemperatureWarning(Time.seconds(10)))
                .print();

        env.execute();
    }

    /**
     * Detect the monotonously rising temperature over a period of time and output a warning.
     */
    private static class MonotonousTemperatureWarning extends KeyedProcessFunction<String, SensorReading, String> {

        /** Timer duration seconds. */
        private final Time duration;
        /** The temperature of the last value. */
        private ValueState<Double> lastTemp;
        /** The timestamp of the timer */
        private ValueState<Long> timerTs;

        public MonotonousTemperatureWarning(Time duration) {
            this.duration = duration;
        }

        @Override
        public void open(Configuration parameters) {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-temperature", Double.class));
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer-timestamp", Long.class));
        }

        @Override
        public void processElement(SensorReading value,
                                   KeyedProcessFunction<String, SensorReading, String>.Context ctx,
                                   Collector<String> out) throws Exception {
            // temperature rise
            if (lastTemp.value() == null || value.getTemperature() > lastTemp.value()) {
                // there is no timer
                if (timerTs.value() == null) {
                    final long ts = ctx.timerService().currentProcessingTime() + duration.toMilliseconds();
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    timerTs.update(ts);
                }
            }

            // temperature drop
            if (lastTemp.value() != null && value.getTemperature() <= lastTemp.value() &&
                    timerTs.value() != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                timerTs.clear();
            }

            // update last temperature
            lastTemp.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx,
                            Collector<String> out) {
            out.collect(String.format("传感器-%s 温度连续%s秒上升", ctx.getCurrentKey(), duration.getSize()));
            timerTs.clear();
        }

        @Override
        public void close() {
            lastTemp.clear();
        }
    }
}
