package com.ychenchen.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 该案例演示OperatorState, Flink官方只提供了一种实现: listState
 * 目标是实现将输入内容每2条打印一次, 条数可配置
 * 通过该案例引出状态后端state backend和checkpoint(用于保证数据安全)
 *
 * @author alexis.yang
 * @since 2021/2/23 11:27 AM
 */
public class A20TestOperatorStateMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.fromElements(Tuple2.of("Spark", 3), Tuple2.of("Hadoop", 5),
                Tuple2.of("Hadoop", 7), Tuple2.of("Spark", 4));

        dataStreamSource.addSink(new A21CustomSink(2)).setParallelism(1);
        env.execute("A20TestOperatorStateMain");
    }
}
