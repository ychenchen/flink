package com.ychenchen.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author alexis.yang
 * @since 2021/2/23 11:35 AM
 */
public class A21CustomSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    // 用于缓存结果数据的
    private List<Tuple2<String, Integer>> bufferElements;
    // 表示内存中数据的大小阈值
    private int threshold;

    // 用于保存内存中的状态信息
    private ListState<Tuple2<String, Integer>> checkpointState;

    public A21CustomSink(int threshold) {
        this.threshold = threshold;
        this.bufferElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        // 可以将接收到的每一条数据保存到任何的存储系统中
        bufferElements.add(value);
        if (bufferElements.size() == threshold) {
            // 简单打印
            System.out.println("自定义格式:" + bufferElements);
            bufferElements.clear();
        }
    }

    // 用于将内存中数据保存到状态中
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointState.clear();
        for (Tuple2<String, Integer> bufferElement : bufferElements) {
            checkpointState.add(bufferElement);
        }
    }

    // 用于在程序恢复的时候从状态中恢复数据到内存
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> tuple2ListStateDescriptor =
                new ListStateDescriptor<>("bufferd -elements", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));
        // 注册一个 operator state
        checkpointState = context.getOperatorStateStore().getListState(tuple2ListStateDescriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointState.get()) {
                bufferElements.add(element);
            }
        }
    }
}
