package com.ychenchen.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求:当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候
 * 计算这些元素的 value 的平均值
 *
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
                .flatMap(new A03CountWindowAverageWithListState())
                .print();
        env.execute("A00TestKeyedStateMain");
    }
}
