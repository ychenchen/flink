package com.ychenchen.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * IN, 输入的数据类型 ACC,自定义的中间状态
 * Tuple2<Integer,Integer>: key: 计算数据的个数 value:计算总值
 * OUT，输出的数据类型
 *
 * @author alexis.yang
 * @since 2021/4/6 2:54 PM
 */
public class A42MyAggregate implements AggregateFunction<Integer, Tuple2<Integer, Integer>, Double> {
    /**
     * 初始化 累加器
     *
     * @return
     */
    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return new Tuple2<>(0, 0);
    }

    /**
     * 针对每个数据的操作
     *
     * @return
     */
    @Override
    public Tuple2<Integer, Integer> add(Integer element, Tuple2<Integer, Integer> accumulator) {
        // 个数+1
        // 总的值累计
        return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + element);
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> accumulator) {
        return (double) accumulator.f1 / accumulator.f0;
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a1, Tuple2<Integer, Integer> b1) {
        return Tuple2.of(a1.f0 + b1.f0, a1.f1 + b1.f1);
    }
}
