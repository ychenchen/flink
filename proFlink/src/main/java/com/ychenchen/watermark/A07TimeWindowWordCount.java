package com.ychenchen.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 使用EventTime处理无序事件
 * (hadoop,1)
 * (hadoop,3)
 * (hadoop,1)
 * 正确结果应该为231,第3个窗口的结果已经准确了,但第一个窗口的结果还有问题.接下来我们引入Watermark机制来解决第一个窗口的问题.
 * 参照A10/A11
 * @author alexis.yang
 * @since 2021/3/14 9:15 AM
 */
public class A07TimeWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStream = env.addSource(new A06TestSource());
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] split = line.split(",");
                return new Tuple2<>(split[0], Long.valueOf(split[1]));
            }
        }).assignTimestampsAndWatermarks(new A08EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new A09SumProcessWindowFunction());

        result.print().setParallelism(1);

        env.execute("A07TimeWindowWordCount");
    }
}
