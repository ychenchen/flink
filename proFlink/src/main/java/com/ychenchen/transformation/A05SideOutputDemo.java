package com.ychenchen.transformation;

import com.ychenchen.source.A10CustomNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 在上一个例子中,我们看到SplitStream被标记为过时了,未来采用SideOutput.
 * https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/side_output/
 * @author alexis.yang
 * @since 2021/2/18 2:13 PM
 */
public class A05SideOutputDemo {
    public static void main(String[] args) throws Exception {
        // 获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<Long> text = env.addSource(new A10CustomNoParalleSource()).setParallelism(1);
        // 对流进行切分，按照数据的奇偶性进行区分
        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
        SingleOutputStreamOperator<Long> mainDataStream = text.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                // emit data to regular output
                out.collect(value);
                // emit data to side output
                if (value % 2 == 0) {
                    ctx.output(outputTag, "even-" + value);
                } else {
                    ctx.output(outputTag, "odd-" + value);
                }
            }
        });

        // 打印结果
        DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                return input.startsWith("even-");
            }
        });

        sideOutputStream.print().setParallelism(1);

        String jobName = A05SideOutputDemo.class.getSimpleName();
        env.execute(jobName);
    }
}
