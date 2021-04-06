package com.ychenchen.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * 总结: GlobalWindow效果跟CountWindow(3)很像，但又有点不像，因为如果是CountWindow(3)，单词每次出现的都是3次，不会包含之前的次数，而这个每次都包含了之前的次数
 *
 * @author alexis.yang
 * @since 2021/4/6 9:48 AM
 */
public class A04GlobalWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>)
                        (line, collector) -> {
                            String[] splits = line.split(",");
                            for (String split : splits) {
                                collector.collect(Tuple2.of(split, 1));
                            }
                        })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));

        stream.keyBy(0)
                .window(GlobalWindows.create())
                //如果不加触发器程序是启动不起来的
                .trigger(CountTrigger.of(3))
                .sum(1)
                .print();

        env.execute("A04GlobalWindow");
    }
}
