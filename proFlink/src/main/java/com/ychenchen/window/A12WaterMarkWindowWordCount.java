package com.ychenchen.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 得到并打印每隔 3 秒钟统计前 3 秒内的相同的 key 的所有的事件
 * 3秒钟统计一次,相当于单词计数
 *
 * -- window计算触发的条件, 一条一条的数据输入
 * 000001,1461756862000
 * 000001,1461756866000
 * 000001,1461756872000
 * 000001,1461756873000
 * 000001,1461756874000
 * 000001,1461756876000
 * 000001,1461756877000
 *
 * 总结:window触发的时间
 * 1. watermark 时间 >= window_end_time
 * 2. 在 [window_start_time, window_end_time) 区间中有数据存在，注意是左闭右开的区间，而且是以 event time 来计算的
 *
 * 处理迟到太多的事件:
 * 1. 丢弃，这个是默认的处理方式
 * 2. allowedLateness 指定允许数据延迟的时间(不推荐使用)
 * 3. sideOutputLateData 收集迟到的数据(大多数企业里面使用的情况)
 *
 * 1. 当我们设置允许迟到 2 秒的事件，第一次 window 触发的条件是 watermark >= window_end_time
 * 2. 第二次(或者多次)触发的条件是 watermark < window_end_time + allowedLateness
 *
 * @author alexis.yang
 * @since 2021/3/14 10:00 AM
 */
public class A12WaterMarkWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 这里将并行度设置为1非常重要,否则程序运行起来看不到预期的结果
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置Watermark的周期为1s
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] split = line.split(",");
                return new Tuple2<>(split[0], Long.valueOf(split[1]));
            }
        }).assignTimestampsAndWatermarks(new A13EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
//                .allowedLateness(Time.seconds(2))   // 允许事件迟到 2 秒
                .process(new A14SumProcessWindowFunction())
                .print().setParallelism(1);

        env.execute("A12WaterMarkWindowWordCount");
    }
}
