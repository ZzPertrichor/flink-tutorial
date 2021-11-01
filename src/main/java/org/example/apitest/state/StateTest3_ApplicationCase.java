package org.example.apitest.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.apitest.bean.SensorReading;

/**
 * An application case of keyed state.
 *
 * @author zm
 */
public class StateTest3_ApplicationCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .socketTextStream("localhost", 8888)
                .map(SensorReading::fromText);

        source.keyBy(SensorReading::getId)
                .flatMap(new TempJumpWarning(10.0))
                .print();

        env.execute();
    }

    /**
     * Detect temperature jump and output warning.
     */
    private static class TempJumpWarning
            extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private final Double threshold;
        private ValueState<Double> lastTempState;

        public TempJumpWarning(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            final Double lastTemp = this.lastTempState.value();
            if (lastTemp != null) {
                if (Math.abs(value.getTemperature() - lastTemp) >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
                }
            }
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() {
            lastTempState.clear();
        }
    }
}
