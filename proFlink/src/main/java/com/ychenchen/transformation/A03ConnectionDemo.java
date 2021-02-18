package com.ychenchen.transformation;

import com.ychenchen.source.A10CustomNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理
 * 方法
 * @author alexis.yang
 * @since 2021/2/18 11:54 AM
 */
public class A03ConnectionDemo {
    public static void main(String[] args) throws Exception {
        // Step1: 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Step2: 获取数据源
        DataStreamSource<Long> source1 = env.addSource(new A10CustomNoParalleSource()).setParallelism(1);//注意:针对此source，并行度只能设置为1
        DataStreamSource<Long> source2 = env.addSource(new A10CustomNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<String> source2Str = source2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            } });

        ConnectedStreams<Long, String> connectedStreams = source1.connect(source2Str);

        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        // 打印结果
        result.print().setParallelism(1);

        String jobName = A02UnionDemo.class.getSimpleName();
        env.execute(jobName);
    }
}
