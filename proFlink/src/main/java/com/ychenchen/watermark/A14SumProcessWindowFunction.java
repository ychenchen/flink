package com.ychenchen.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author alexis.yang
 * @since 2021/3/14 10:47 AM
 */
public class A14SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
        System.out.println("处理时间:" + dateFormat.format(context.currentProcessingTime()));
        System.out.println("window start time : " + dateFormat.format(context.window().getStart()));
        List<String> list = new ArrayList<>();
        for (Tuple2<String, Long> ele : elements) {
            list.add(ele.toString() + "|" + dateFormat.format(ele.f1));
        }
        out.collect(list.toString());
        System.out.println("window end time : " + dateFormat.format(context.window().getEnd()));
    }
}
