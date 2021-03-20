package com.ychenchen.watermark;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 需求:每隔5秒计算最近10秒的单词出现的次数(接口调用出错的次数)
 * 自定义source，模拟:第 13 秒的时候连续发送 2 个事件，第 16 秒的时候再发送 1 个事件
 * @author alexis.yang
 * @since 2021/3/14 7:22 AM
 */
public class A04ProcessWindowTimeOrderedUnOrdered {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.addSource(new A06TestSource()); //A05TestSource->Ordered; A06TestSource->UnOrdered
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(",");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new A03SumProcessWindowFunction());

        result.print().setParallelism(1);
        env.execute("A04ProcessWindowTimeOrdered");

    }
}

