package com.ychenchen.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * @author alexis.yang
 * @since 2021/4/6 10:56 AM
 */
public class A23MyCountEvictor implements Evictor<Tuple2<String, Integer>, GlobalWindow> {

    // window 的大小
    private long windowCount;

    public A23MyCountEvictor(long windowCount) {
        this.windowCount = windowCount;
    }

    /**
     * 在 window 计算之前删除特定的数据
     * @param elements window 中所有的元素
     * @param size window 中所有元素的大小
     * @param window window
     * @param evictorContext 上下文
     */
    @Override
    public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
        if (size <= windowCount) {
            return;
        } else {
            int evictedCount = 0;
            for (Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator(); iterator.hasNext();){
                iterator.next();
                evictedCount++;
                if (evictedCount > size - windowCount) { // size - windowCount表示多出来的元素个数, 即需要驱逐的个数, 即驱逐evictedCount小于等于这个数字的数据.
                    break;
                } else {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * 在 window 计算之后删除特定的数据
     * @param elements window 中所有的元素
     * @param size window 中所有元素的大小
     * @param window
     * @param evictorContext
     */
    @Override
    public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

    }
}
