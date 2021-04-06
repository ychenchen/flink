package com.ychenchen.window;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * window全量聚合
 * 等属于窗口的数据到齐,才开始进行聚合计算,可以实现对窗口内的数据进行排序等需求
 * apply(windowFunction)
 * process(processWindowFunction)
 * processWindowFunction 比windowFunction提供了更多的上下文信息。类似于map和RichMap的关系
 *
 * @author alexis.yang
 * @since 2021/4/6 3:17 PM
 */
public class A51SocketDemoFullAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));
        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.timeWindowAll(Time.seconds(10));
        windowResult.process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> elements, Collector<Integer> collector) throws Exception {
                System.out.println("执行计算逻辑");
                int count = 0;
                Iterator<Integer> integerIterator = elements.iterator();
                while (integerIterator.hasNext()) {
                    Integer number = integerIterator.next();
                    count += number;
                }
                collector.collect(count);
            }
        }).print();

        env.execute(A51SocketDemoFullAgg.class.getSimpleName());
    }
}
