package com.ychenchen.transformation;

import com.ychenchen.source.A10CustomNoParalleSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 根据规则把一个数据流切分为多个流
 * 应用场景:
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在
 * 根据一定的规则，把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 * @author alexis.yang
 * @since 2021/2/18 1:47 PM
 */
public class A04SplitDemo {
    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<Long> text = env.addSource(new A10CustomNoParalleSource()).setParallelism(1);
        // 对流进行切分，按照数据的奇偶性进行区分
        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    outPut.add("even");
                } else {
                    outPut.add("odd");
                }
                return outPut;
            }
        });

        //选择一个或者多个切分后的流
        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> oddStream = splitStream.select("odd");
        DataStream<Long> moreStream = splitStream.select("even", "odd");

        //打印结果
        evenStream.print().setParallelism(1);
        String jobName = A04SplitDemo.class.getSimpleName();
        env.execute(jobName);
    }
}
