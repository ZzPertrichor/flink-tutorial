package org.example.apitest.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 */
public class StateTest2_KeyedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .socketTextStream("localhost", 8888)
                .map(SensorReading::fromText);

        // 定义一个有状态的map操作，统计当前sensor数据个数
        source.keyBy(SensorReading::getId)
                .map(new countFunction())
                .print();

        env.execute();
    }

    private static class countFunction extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("key-count", Integer.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            final Integer current = count.value();
            count.update(current == null ? 1 : current + 1);
            return count.value();
        }
    }
}
