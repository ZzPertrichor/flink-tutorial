package org.example.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.apitest.bean.SensorReading;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * self define SourceFunction
 *
 * @author zm
 * @since 2021-10-20
 */
public class SourceTest4_DSF {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> dataStream = env.addSource(new SensorSource());
        dataStream.print();
        env.execute();
    }

    public static class SensorSource implements SourceFunction<SensorReading> {

        /** Flag to make the source cancelable. */
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Map<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + i,
                        10 + ThreadLocalRandom.current().nextGaussian() * 10);
            }

            while (running) {
                sensorTempMap.keySet().parallelStream().forEach(sensorId -> {
                    final double newTemp = sensorTempMap.get(sensorId) +
                            ThreadLocalRandom.current().nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                });
                // noinspection BusyWait
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
