package com.ychenchen.state;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

/**
 * @author alexis.yang
 * @since 2021/2/19 3:36 PM
 */
public class A04CountWindowAverageWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    //1. MapState :key 是一个唯一的值，value 是接收到的相同的 key 对应的 value 的值
    private transient MapState<String, Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<String, Long>(
                "average",
                String.class, Long.class);

        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> collector) throws Exception {
        mapState.put(UUID.randomUUID().toString(), element.f1);
        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        List<Long> allElements = Lists.newArrayList(mapState.values().iterator());
        if (allElements.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Long ele : allElements) {
                count++;
                sum += ele;
            }
            double avg = (double) sum / count;
            collector.collect(Tuple2.of(element.f0, avg));
            // 清除状态
            mapState.clear();
        }
    }
}
