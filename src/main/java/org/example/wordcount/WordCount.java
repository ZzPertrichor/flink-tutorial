package org.example.wordcount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 批处理 word count
 *
 * @author zm
 * @since 2021-10-19
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inputPath = "C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\hello.txt";
        final DataSet<String> dataSet = env.readTextFile(inputPath);
        // 对数据集进行处理，按空格分词，转换成(word, 1)二元组进行统计
        dataSet.flatMap(WordCount::flatMap)
                .groupBy(0)
                .sum(1)
                .print();
    }

    public static void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        // 按空格分词
        final String[] words = value.split(" ");
        // 遍历所有word，包成二元组输出
        Arrays.stream(words).forEach(word -> out.collect(new Tuple2<>(word, 1)));
    }
}
