package com.ychenchen.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2021/2/19 5:01 PM
 */
public class A06ContainsValueFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {

    private AggregatingState<Long, String> totalStr;

    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Long, String, String> descriptor = new AggregatingStateDescriptor<Long, String, String>(
                "totalStr", // 状态的名字
                new AggregateFunction<Long, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return "Contains: ";
                    }

                    @Override
                    public String add(Long value, String accumulator) {
                        if ("Contains: ".equals(accumulator)) {
                            return accumulator + value;
                        }
                        return accumulator + " and " + value;
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a + " and " + b;
                    }
                },
                String.class    // 状态存储的数据类型
        );
        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, String>> collector) throws Exception {
        totalStr.add(element.f1);
        collector.collect(Tuple2.of(element.f0, totalStr.get()));
    }
}
