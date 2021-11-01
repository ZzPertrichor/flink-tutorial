package org.example.apitest.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 */
public class StateTest1_OperatorState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .socketTextStream("localhost", 8888)
                .map(SensorReading::fromText);

        // 定义一个有状态的map操作，统计当前分区数据个数
        source.map(new CountFunction())
                .print();

        env.execute();
    }

    private static class CountFunction implements MapFunction<SensorReading, Integer>, CheckpointedFunction {
        private ListState<Integer> state;
        // 定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // the keyed state is always up-to-date anyways
            // just bring the per-partition state
            state.clear();
            state.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // get the state data structure for the per-partition state
            state = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("perPartitionCount", Integer.class));

            // initialize the "local count variable" based on the operator state
            for (Integer i : state.get()) {
                count += i;
            }
        }
    }
}
