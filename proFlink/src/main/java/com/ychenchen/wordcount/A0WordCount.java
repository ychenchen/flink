package com.ychenchen.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2020/12/13 10:01 PM
 * @desciption 1.最简单版本WordCount
 */
public class A0WordCount {
    public static void main(String[] args) throws Exception{
        // 步骤一，获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 步骤二，获取数据源
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        // 步骤三：数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = s.split(",");
                for (String word : fields) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0)
                .sum(1);
        // 步骤四：数据输出
        result.print();
        // 步骤五：启动任务
        env.execute("word count");
    }
}
