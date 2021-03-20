package com.ychenchen.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author alexis.yang
 * @since 2021/3/14 10:10 AM
 */
public class A21EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
    private long currentMaxEventTime = 0L;
    private long maxOutOfOrderness = 10000; // 最大允许的乱序时间 10 秒

    // 拿到每一个事件的 Event Time
    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
        long currentElementEventTime = element.f1;
        currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);
        //打印线程
        long id = Thread.currentThread().getId();
        System.out.println("当前线程ID:" + id + "event = " + element
                + "|" + dateFormat.format(element.f1) // Event Time
                + "|" + dateFormat.format(currentMaxEventTime) // Max EventTime
                + "|" + dateFormat.format(getCurrentWatermark().getTimestamp())); // Current Watermark
        return currentElementEventTime;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        /**
         * WasterMark会周期性的产生，默认就是每隔200毫秒产生一个 *
         * 设置 watermark 产生的周期为 1000ms
         * env.getConfig().setAutoWatermarkInterval(1000); */
        // window延迟5秒触发
//        System.out.println("water mark...");
        return new Watermark(currentMaxEventTime - maxOutOfOrderness);
    }

}
