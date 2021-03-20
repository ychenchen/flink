package com.ychenchen.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author alexis.yang
 * @since 2021/3/14 9:17 AM
 */
public class A11EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - 5000);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
        return element.f1;
    }
}
