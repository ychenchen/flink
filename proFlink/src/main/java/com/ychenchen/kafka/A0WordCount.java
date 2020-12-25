package com.ychenchen.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author alexis.yang
 * @since 2020/12/22 2:01 PM
 */
public class A0WordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs
                                        // If checkpointing is not enabled, the Kafka consumer will periodically commit the offsets to Zookeeper.
        String topic="alexis";
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers","192.168.6.213:9092");
        consumerProperties.setProperty("group.id","alexisConsumer");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), consumerProperties);
//        myConsumer.setStartFromEarliest();     // start from the earliest record possible
//        myConsumer.setStartFromLatest();       // start from the latest record
        myConsumer.setStartFromGroupOffsets(); // the default behaviour

        DataStreamSource<String> data = env.addSource(myConsumer).setParallelism(3);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneStream = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = line.split(",");
                for (String field : fields) {
                    collector.collect(Tuple2.of(field, 1));
                }
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordOneStream.keyBy(0).sum(1).setParallelism(2);

        result.map(tuples -> tuples.toString()).setParallelism(2).print().setParallelism(1);

        env.execute("Kafka Word Count");
    }
}
