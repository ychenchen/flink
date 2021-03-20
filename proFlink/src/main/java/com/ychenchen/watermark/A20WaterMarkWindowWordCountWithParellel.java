package com.ychenchen.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 多并行度下的WaterMark:
 * 一个window可能会接受到多个waterMark，我们以最小的为准
 *
 * 输入数据:
 * 000001,1461756870000
 * 000001,1461756883000
 * 000001,1461756888000
 *
 * @author alexis.yang
 * @since 2021/3/14 2:04 PM
 */
public class A20WaterMarkWindowWordCountWithParellel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);  // 并行度设为2,演示多并行度下的Watermark.
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
        }).assignTimestampsAndWatermarks(new A21EventTimeExtractor())
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

        env.execute("A20WaterMarkWindowWordCountWithParellel");
    }
}
