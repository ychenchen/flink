package com.ychenchen.state;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ychenchen.state.A11OrderInfo1.string2OrderInfo1;
import static com.ychenchen.state.A12OrderInfo2.string2OrderInfo2;

/**
 * 将两个流中订单号一样的数据合并在一起输出
 *
 * @author alexis.yang
 * @since 2021/2/23 10:08 AM
 */
public class A14OrderStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> info1 = env.addSource(new A13FileSource(A10Constants.ORDER_INFO1_PATH));
        DataStreamSource<String> info2 = env.addSource(new A13FileSource(A10Constants.ORDER_INFO2_PATH));

        KeyedStream<A11OrderInfo1, Long> a11OrderInfo1LongKeyedStream = info1.map(line -> string2OrderInfo1(line))
                .keyBy(a11OrderInfo1 -> a11OrderInfo1.getOrderId());
        KeyedStream<A12OrderInfo2, Long> a12OrderInfo2LongKeyedStream = info2.map(line -> string2OrderInfo2(line))
                .keyBy(a12OrderInfo2 -> a12OrderInfo2.getOrderId());

        a11OrderInfo1LongKeyedStream.connect(a12OrderInfo2LongKeyedStream)
                .flatMap(new A15EnrichmentFunction())
                .print();

        env.execute("A14OrderStream");
    }
}
