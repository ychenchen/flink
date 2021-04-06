package com.ychenchen.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author alexis.yang
 * @since 2021/4/6 9:45 AM
 */
public class A03SessionWindow {
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

        stream.keyBy(0)
                // Session Window
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute("A03SessionWindow");
    }
}
