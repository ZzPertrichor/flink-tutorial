package org.example.wordcount;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 流处理 word count
 *
 * @author zm
 * @since 2021-10-19
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 用 parameter tool 工具从程序启动参数中提取配置项
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final int parallelism = parameterTool.getInt("parallelism");
        final String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(parallelism);
        final DataStream<String> dataStream = env.socketTextStream(host, port);
        dataStream.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    final String[] words = value.split(" ");
                    Arrays.stream(words).forEach(word -> out.collect(new Tuple2<>(word, 1)));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(value -> value.getField(0))
                .sum(1)
                .print();
        env.execute();
    }
}
