package com.ychenchen.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author alexis.yang
 * @since 2021/3/13 10:33 PM
 */
public class A03SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {
    FastDateFormat dataFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        System.out.println("当前系统的间:" + dataFormat.format(System.currentTimeMillis()));
        System.out.println("Window的处理时间:" + dataFormat.format(context.currentProcessingTime()));
        System.out.println("Window的开始时间:" + dataFormat.format(context.window().getStart()));
        System.out.println("Window的结束时间:" + dataFormat.format(context.window().getEnd()));
        int sum = 0;
        for (Tuple2<String, Integer> ele : elements) {
            sum += 1;
        }
        // 输出单词出现的次数
        out.collect(Tuple2.of(tuple.getField(0), sum));
    }
}
