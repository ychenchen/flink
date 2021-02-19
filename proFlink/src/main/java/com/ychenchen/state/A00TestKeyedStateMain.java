package com.ychenchen.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author alexis.yang
 * @since 2021/2/19 2:38 PM
 */
public class A00TestKeyedStateMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource
                .keyBy(0)
                .flatMap(new A01CountWindowAverageWithValueState())
                .print();
        env.execute("A00TestKeyedStateMain");
    }
}
