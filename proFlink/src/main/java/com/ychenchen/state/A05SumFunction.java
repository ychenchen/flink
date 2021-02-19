package com.ychenchen.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2021/2/19 4:54 PM
 */
public class A05SumFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    // managed keyed state
    // 用于保存每一个 key 对应的 value 的总值
    private ReducingState<Long> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<>(
                "sum",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                        return value1 + value2;
                    }
                }, Long.class// 状态存储的数据类型
        );

        sumState = getRuntimeContext().getReducingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> collector) throws Exception {
        // 把数据放入到state中
        sumState.add(element.f1);
        collector.collect(Tuple2.of(element.f0, sumState.get()));
    }
}
