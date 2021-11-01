package org.example.apitest.sink;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.apitest.bean.SensorReading;

import java.util.HashMap;

/**
 * @author zm
 */
public class SinkTest2_Redis {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<SensorReading> source = env
                .readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .map(SensorReading::fromText);

        source.addSink(new RedisSink());
        env.execute();
    }

    public static class RedisSink extends RichSinkFunction<SensorReading> {
        private RedisClient redisClient;
        private StatefulRedisConnection<String, String> connection;

        @Override
        public void open(Configuration parameters) {
            redisClient = RedisClient.create("redis://zmhqx123@localhost:6379/0");
            connection = redisClient.connect();
        }

        @Override
        public void invoke(SensorReading value, Context context) {
            final HashMap<String, String> map = new HashMap<>();
            map.put(value.getId(), value.getTemperature().toString());
            connection.sync().hset("sensor_temp", map);
        }

        @Override
        public void close() {
            connection.close();
            redisClient.shutdown();
        }
    }
}
