package com.ychenchen.wordcount;

import com.ychenchen.dto.WordAndCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2020/12/14 1:03 PM
 * @desciption 2.使用面向对象，将结果改为Java对象表示，遇到结果字段较多的时候比较方便
 */
public class A1WordCountWithPojo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
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
