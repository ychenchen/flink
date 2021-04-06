package com.ychenchen.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 需求:求每个窗口里面的数据的平均值
 *
 * @author alexis.yang
 * @since 2021/4/6 2:52 PM
 */
public class A41AggregateWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));
        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.timeWindowAll(Time.seconds(5));
        windowResult.aggregate(new A42MyAggregate()).print();

        env.execute(A41AggregateWindowTest.class.getSimpleName());
    }
}
