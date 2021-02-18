package com.ychenchen.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 通过实现sourceFunction接口来自定义单并行度的数据源, 通过state记录当前的count数
 * @author alexis.yang
 * @since 2020/12/25 12:51 PM
 */
public class A11CustomNoParalleCountSource implements SourceFunction<Long>, CheckpointedFunction {
    private long count = 0L;
    private volatile boolean isRunning = true;

    private transient ListState<Long> checkpointedCount;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning && count < 100) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(count);
                count ++;
            }

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("count", Long.class));

        if (context.isRestored()) {
            for (Long count : this.checkpointedCount.get()) {
                this.count = count;
            }
        }
    }
}
