package com.ychenchen.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author alexis.yang
 * @since 2020/12/25 12:59 PM
 */
public class A12StreamingFromCustomNoParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> numberStream = env.addSource(new A11CustomNoParalleCountSource()).setParallelism(1);

        SingleOutputStreamOperator<Long> dataStream = numberStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                System.out.println("接收到了数据：" + aLong);
                return aLong;
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Long> filtered = dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong % 2 == 0;
            }
        });

        filtered.print().setParallelism(1);

        env.execute("A11StreamingFromCustomNoParallelSource");
    }
}
