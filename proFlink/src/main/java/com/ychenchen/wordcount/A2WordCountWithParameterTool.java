package com.ychenchen.wordcount;

import com.ychenchen.dto.WordAndCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2020/12/14 1:03 PM
 * @desciption 2.使用面向对象，将结果改为Java对象表示，遇到结果字段较多的时候比较方便
 */
public class A2WordCountWithParameterTool {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过Flink提供的工具类获取传入的参数
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("/data/code/github/ychenchen/flink/proFlink/src/main/resources/parameters.properties");
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> data = env.socketTextStream(host, port);
        SingleOutputStreamOperator<WordAndCount> wordCount = data.flatMap(new FlatMapFunction<String, WordAndCount>() {
            @Override
            public void flatMap(String s, Collector<WordAndCount> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(new WordAndCount(field, 1));
                }
            }
        }).keyBy("word").sum("count");
        wordCount.print();
        env.execute("wordCountWithPOJO");
    }
}
