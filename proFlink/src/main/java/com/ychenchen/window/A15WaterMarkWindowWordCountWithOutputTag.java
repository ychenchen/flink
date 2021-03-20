package com.ychenchen.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 得到并打印每隔 3 秒钟统计前 3 秒内的相同的 key 的所有的事件
 * 3秒钟统计一次,相当于单词计数
 * <p>
 * -- window计算触发的条件, 一条一条的数据输入
 * hadoop,1461756870000
 * hadoop,1461756883000
 * 迟到的数据
 * hadoop,1461756870000
 * hadoop,1461756871000
 * hadoop,1461756872000
 *
 * 收集迟到的数据
 *
 * @author alexis.yang
 * @since 2021/3/14 10:00 AM
 */
public class A15WaterMarkWindowWordCountWithOutputTag {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 这里将并行度设置为1非常重要,否则程序运行起来看不到预期的结果
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置Watermark的周期为1s
        env.getConfig().setAutoWatermarkInterval(1000);

        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data") {

        };

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> result = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] split = line.split(",");
                return new Tuple2<>(split[0], Long.valueOf(split[1]));
            }
        }).assignTimestampsAndWatermarks(new A13EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
//                .allowedLateness(Time.seconds(2))   // 允许事件迟到 2 秒
                .sideOutputLateData(outputTag)  // 保存迟到太多的数据
                .process(new A14SumProcessWindowFunction());

        result.print().setParallelism(1);

        // 获取迟到太多的数据
        SingleOutputStreamOperator<String> lateDataStream = result.getSideOutput(outputTag).map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return "迟到的数据: " + stringLongTuple2.toString();
            }
        });

        lateDataStream.print();

        env.execute("A15WaterMarkWindowWordCountWithOutputTag");
    }
}
