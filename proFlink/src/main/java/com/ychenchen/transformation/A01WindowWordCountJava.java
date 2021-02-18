package com.ychenchen.transformation;

import com.ychenchen.dto.WordAndCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *
 * 需求:每隔1秒计算最近2秒单词出现的次数
 * 练习算子:
 * flatMap
 * keyBy:
 *      dataStream.keyBy("someKey") // 指定对象中的 "someKey"字段作为分组key
 *      dataStream.keyBy(0) //指定Tuple中的第一个元素作为分组key
 * sum
 *
 * @author alexis.yang
 * @since 2020/12/25 1:55 PM
 */
public class A01WindowWordCountJava {
    public static void main(String[] args) throws Exception {
        // Step0: 准备参数
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            port = 9999;
            System.err.println("no port set,user default 9999");
        }
        String hostname="localhost";
        String delimiter="\n";
        // Step1: 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Step2: 获取Socket数据源
        DataStreamSource<String> source = env.socketTextStream(hostname, port, delimiter);
        // Step3: 处理数据
        SingleOutputStreamOperator<WordAndCount> wordCountStream = source.flatMap((FlatMapFunction<String, WordAndCount>) (line, collector) -> {
            String[] fields = line.split(",");
            for (String field : fields) {
                collector.collect(new WordAndCount(field, 1));
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))   // 每隔1秒计算最近2秒的数据
                .sum("count");

        // Step4: 打印并设置并行度
        wordCountStream.print().setParallelism(1);

        env.execute("WindowWordCountJava");
    }
}
