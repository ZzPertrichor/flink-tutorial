package org.example.apitest.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.example.apitest.bean.SensorReading;

import java.time.Duration;

/**
 * @author zm
 */
public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        final DataStream<SensorReading> source = env
                .socketTextStream("localhost", 8888)
                .map(SensorReading::fromText);

        final OutputTag<SensorReading> late = new OutputTag<>("late");
        final SingleOutputStreamOperator<SensorReading> minTempStream = source
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        // 升序数据
                        /*.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((element, timestamp) -> element.getTimestamp() * 1000L)*/
                        // 乱序数据
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000L))
                // 基于事件时间的开窗聚合，统计15秒内温度的最小值
                .keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .minBy("temperature");
        minTempStream.print("minTemp");
        minTempStream.getSideOutput(late).print("late");

        env.execute();
    }
}
