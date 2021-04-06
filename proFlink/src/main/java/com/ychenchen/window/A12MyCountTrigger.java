package com.ychenchen.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;


/**
 * @author alexis.yang
 * @since 2021/4/6 9:53 AM
 */
public class A12MyCountTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {

    // 表示指定的元素的最大的数量
    private long maxCount;

    // 用于存储每个 key 对应的 count 值
    private ReducingStateDescriptor<Long> stateDescriptor
            = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return aLong + t1;
        }
    }, Long.class);


    public A12MyCountTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    /**
     * 当一个元素进入到一个 window 中的时候就会调用这个方法 * @param element 元素
     *
     * @param timestamp 进来的时间
     * @param window    元素所属的窗口注:效果跟CountWindow一模一样
     * @param ctx       上下文
     * @return TriggerResult
     * 1. TriggerResult.CONTINUE :表示对 window 不做任何处理
     * 2. TriggerResult.FIRE :表示触发 window 的计算
     * 3. TriggerResult.PURGE :表示清除 window 中的所有数据
     * 4. TriggerResult.FIRE_AND_PURGE :表示先触发 window 计算，然后删除
     * window 中的数据
     * @throws Exception
     */
    @Override
    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        // 拿到当前 key 对应的 count 状态值
        ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
        // count 累加 1
        count.add(1L);
        // 如果当前 key 的 count 值等于 maxCount
        if (count.get() == maxCount) {
            count.clear();
            // 触发 window 计算，删除数据
            return TriggerResult.FIRE_AND_PURGE;
        }
        // 否则，对 window 不做任何的处理
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        // 写基于 Processing Time 的定时器任务逻辑
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        // 写基于 Event Time 的定时器任务逻辑
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        // 清除状态值
        ctx.getPartitionedState(stateDescriptor).clear();
    }
}
