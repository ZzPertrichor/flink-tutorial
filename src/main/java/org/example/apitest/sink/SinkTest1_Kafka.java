package org.example.apitest.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.apitest.bean.SensorReading;

/**
 * @author zm
 * @since 2021-10-22
 */
public class SinkTest1_Kafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final DataStream<String> source = env
                .readTextFile("C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .map(SensorReading::fromText)
                .map(SensorReading::toString);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("172.18.76.109:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("topic01")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        source.sinkTo(sink);
        env.execute();
    }
}
