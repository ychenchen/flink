package com.ychenchen.sink;

import com.ychenchen.source.A10CustomNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.apache.flink.core.fs.Path;

import java.util.concurrent.TimeUnit;

/**
 * @author alexis.yang
 * @since 2021/2/18 2:53 PM
 */
public class WriteAsTextDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.AT_LEAST_ONCE);
        DataStreamSource<Long> numberStream = env.addSource(new A10CustomNoParalleSource()).setParallelism(1);
        SingleOutputStreamOperator<Long> dataStream = numberStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接受到了数据:" + value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> filter = dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long number) throws Exception {
                return number % 2 == 0;
            }
        });
        String outputPath = "/Users/alexis";
//        filter.writeAsText(outputPath).setParallelism(1);
        // writeAsText被标记为过时了,使用StreamingFileSink替代它
        // https://ci.apache.org/projects/flink/flink-docs-master/docs/connectors/datastream/streamfile_sink/
        // 需要注意的是: 使用StreamingFileSink必须开启CheckPoint, 否则落地文件名会一直处在.part-0-0.inprogress.581849fe-270c-4402-b24e-83e48cfe712e状态,
        // 只有完成CheckPoint的文件才会真实落地.
        final StreamingFileSink<Long> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Long>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 100)
                                .build())
                .build();
        filter.addSink(sink);

        String jobName = WriteAsTextDemo.class.getSimpleName();
        env.execute(jobName);
    }
}
