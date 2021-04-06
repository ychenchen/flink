package com.ychenchen.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * window增量聚合
 * 窗口中每进入一条数据，就进行一次计算，等时间到了展示最后的结果
 * <p>
 * 常用的聚合算子
 * reduce(reduceFunction)
 * aggregate(aggregateFunction)
 * sum(),min(),max()
 *
 * @author alexis.yang
 * @since 2021/4/6 2:39 PM
 */
public class A31SocketDemoIncrAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));
        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.timeWindowAll(Time.seconds(10));
        windowResult.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer last, Integer current) throws Exception {
                System.out.println("执行逻辑" + last + " " + current);
                return last + current;
            }
        }).print();

        env.execute(A31SocketDemoIncrAgg.class.getSimpleName());
    }
}
