package com.ychenchen.transformation;

import com.ychenchen.source.A10CustomNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 合并多个流，新的流会包含所有流中的数据，但是union有一个限制，就是所有合并的流类型必须是一致
 *
 * @author alexis.yang
 * @since 2021/2/18 11:39 AM
 */
public class A02UnionDemo {
    public static void main(String[] args) throws Exception {
        // Step1: 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Step2: 获取数据源
        DataStreamSource<Long> source1 = env.addSource(new A10CustomNoParalleSource()).setParallelism(1);//注意:针对此source，并行度只能设置为1
        DataStreamSource<Long> source2 = env.addSource(new A10CustomNoParalleSource()).setParallelism(1);
        // 将两个数据源组装到一起
        DataStream<Long> text = source1.union(source2);
        // 打印出原始数据
        SingleOutputStreamOperator<Long> number = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到原始数据: " + value);
                return value;
            }
        });

        // 每2秒处理一次数据
        SingleOutputStreamOperator<Long> sum = number.timeWindowAll(Time.seconds(2)).sum(0);
        // 打印结果
        sum.print().setParallelism(1);
        String jobName = A02UnionDemo.class.getSimpleName();

        env.execute(jobName);
    }
}
