package com.ychenchen.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2021/3/14 9:30 AM
 */
public class A09SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Integer>, Tuple, TimeWindow> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        int sum = 0;
        for (Tuple2<String, Long> ele : elements) {
            sum += 1;
        }
        // 输出单词出现的次数
        out.collect(Tuple2.of(tuple.getField(0), sum));
    }
}
