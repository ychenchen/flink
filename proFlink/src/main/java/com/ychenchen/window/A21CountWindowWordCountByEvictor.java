package com.ychenchen.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 每隔2个元素统计最近3个元素,但测试发现程序有问题,不符合预期结果.
 * @author alexis.yang
 * @since 2021/4/6 10:54 AM
 */
public class A21CountWindowWordCountByEvictor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>)
                        (line, collector) -> {
                            String[] splits = line.split(",");
                            for (String split : splits) {
                                collector.collect(Tuple2.of(split, 1));
                            }
                        })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));

        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> keyedWindow = stream
                .keyBy(0)
                .window(GlobalWindows.create())
                .trigger(new A22MyCountTrigger(2))
                .evictor(new A23MyCountEvictor(3));

        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);
        wordCounts.print().setParallelism(1);

        env.execute("A11Trigger");
    }
}
