package com.ychenchen.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * None Keyed Window. VS Keyed Window.
 *
 * @author alexis.yang
 * @since 2021/4/6 9:35 AM
 */
public class A01WindowType {
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
                        });

        // None Keyed Window.
        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> noneKeyedStream = stream.timeWindowAll(Time.seconds(3));
        noneKeyedStream.sum(1).print();

        // Keyed Window.
        stream.keyBy(0)
                .timeWindow(Time.seconds(3))
                .sum(1);

        env.execute("A01WindowType");
    }
}
