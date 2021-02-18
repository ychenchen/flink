package com.ychenchen.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 通过实现sourceFunction接口来自定义单并行度的数据源
 * @author alexis.yang
 * @since 2020/12/25 12:51 PM
 */
public class A10CustomNoParalleSource implements SourceFunction<Long> {
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
