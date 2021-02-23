package com.ychenchen.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2021/2/23 10:24 AM
 */
public class A15EnrichmentFunction extends RichCoFlatMapFunction<A11OrderInfo1, A12OrderInfo2, Tuple2<A11OrderInfo1, A12OrderInfo2>> {

    // 定义第一个流 key对应的state
    private ValueState<A11OrderInfo1> a11OrderInfo1ValueState;

    // 定义第二个流 key对应的state
    private ValueState<A12OrderInfo2> a12OrderInfo2ValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        a11OrderInfo1ValueState = getRuntimeContext().getState(new ValueStateDescriptor<A11OrderInfo1>("info1", A11OrderInfo1.class));
        a12OrderInfo2ValueState = getRuntimeContext().getState(new ValueStateDescriptor<A12OrderInfo2>("info2", A12OrderInfo2.class));
    }

    @Override
    public void flatMap1(A11OrderInfo1 value, Collector<Tuple2<A11OrderInfo1, A12OrderInfo2>> out) throws Exception {
        A12OrderInfo2 value2 = a12OrderInfo2ValueState.value();
        if (value2 != null) {
            a12OrderInfo2ValueState.clear();
            out.collect(Tuple2.of(value, value2));
        } else {
            a11OrderInfo1ValueState.update(value);
        }
    }

    @Override
    public void flatMap2(A12OrderInfo2 value, Collector<Tuple2<A11OrderInfo1, A12OrderInfo2>> out) throws Exception {
        A11OrderInfo1 value1 = a11OrderInfo1ValueState.value();
        if (value1 != null) {
            a11OrderInfo1ValueState.clear();
            out.collect(Tuple2.of(value1, value));
        } else {
            a12OrderInfo2ValueState.update(value);
        }
    }
}
