package com.ychenchen.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/state/state.html
 * ValueState<T> :这个状态为每一个 key 保存一个值
 * value() 获取状态值
 * update() 更新状态值
 * clear() 清除状态
 *
 * @author alexis.yang
 * @since 2021/2/18 5:32 PM
 */
public class A01CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    // 用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
    // managed keyed state
    // ValueState 保存的是对应的一个 key 的一个状态值.
    private ValueState<Tuple2<Long, Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<Tuple2<Long, Long>>(
                "average",
                Types.TUPLE(Types.LONG, Types.LONG)
        );
        countAndSum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> collector) throws Exception {
        // 拿到当前key的状态值
        Tuple2<Long, Long> currentState = countAndSum.value();
        // 如果状态没有初始化,先初始化
        if (currentState == null) {
            currentState = Tuple2.of(0L, 0L);
        }
        // 更新状态值中的元素的个数
        currentState.f0 += 1;
        // 更新状态值中的总值
        currentState.f1 += element.f1;
        // 更新状态
        countAndSum.update(currentState);

        // 如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        if (currentState.f0 >= 3) {
            double avg = (double) currentState.f1 / currentState.f0;
            // 输出 key 及其对应的平均值
            collector.collect(Tuple2.of(element.f0, avg));
            // 清空状态值
            countAndSum.clear();
        }
    }
}
