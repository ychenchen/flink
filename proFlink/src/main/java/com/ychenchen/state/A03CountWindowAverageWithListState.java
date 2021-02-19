package com.ychenchen.state;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * @author alexis.yang
 * @since 2021/2/19 2:55 PM
 */
public class A03CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    // ListState保存的是对应的一个key出现的所有的元素
    private transient ListState<Tuple2<Long, Long>> elementsByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Tuple2<Long, Long>> descriptor = new ListStateDescriptor<>(
                "average",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                })
        );
        elementsByKey = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> collector) throws Exception {
        // 拿到当前key的状态值
        Iterable<Tuple2<Long, Long>> currentState = elementsByKey.get();
        if (currentState == null) {
            elementsByKey.addAll(Collections.EMPTY_LIST);
        }
        // 更新状态
        elementsByKey.add(element);

        // 如果当前的 key 出现了 3 次，则计算平均值，并且输出
        List<Tuple2<Long, Long>> allElements = Lists.newArrayList(elementsByKey.get().iterator());
        if (allElements.size() == 3) {
            long count = 0;
            long sum = 0;
            for (Tuple2<Long, Long> ele : allElements) {
                count ++;
                sum += ele.f1;
            }
            double avg = (double) sum / count;
            collector.collect(Tuple2.of(element.f0, avg));
            // 清除状态
            elementsByKey.clear();
        }
    }
}
