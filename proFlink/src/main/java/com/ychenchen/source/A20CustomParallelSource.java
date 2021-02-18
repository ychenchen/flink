package com.ychenchen.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 通过实现ParallelSourceFunction自定义支持并行度的数据源
 * @author alexis.yang
 * @since 2020/12/25 1:41 PM
 */
public class A20CustomParallelSource implements ParallelSourceFunction<Long> {  // Long: 自定义数据源产生的数据的类型
    private long count = 0L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
