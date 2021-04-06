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
 * 需求: 自定义一个CountWindow
 *
 * @author alexis.yang
 * @since 2021/4/6 9:50 AM
 */
public class A11Trigger {
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
                .trigger(new A12MyCountTrigger(3));

        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);
        wordCounts.print().setParallelism(1);

        env.execute("A11Trigger");
    }
}






