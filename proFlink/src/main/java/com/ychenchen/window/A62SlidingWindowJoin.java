package com.ychenchen.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * window join
 * 两个window之间可以进行join，join操作只支持三种类型的window:滚动窗口，滑动窗口，会话窗口
 * 使用方式:
 * stream.join(otherStream) //两个流进行关联
 * .where(<KeySelector>) //选择第一个流的key作为关联字段
 * .equalTo(<KeySelector>)//选择第二个流的key作为关联字段
 * .window(<WindowAssigner>)//设置窗口的类型
 * .apply(<JoinFunction>) //对结果做操作 process apply = foreachWindow
 *
 * @author alexis.yang
 * @since 2021/4/6 3:30 PM
 */
public class A62SlidingWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> orangeStream0 = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> greenStream0 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> orangeStream = orangeStream0.map(number -> Integer.valueOf(number));
        SingleOutputStreamOperator<Integer> greenStream = greenStream0.map(number -> Integer.valueOf(number));

//        orangeStream.join(greenStream)
//                .where( < KeySelector >)
//                .equalTo( < KeySelector >)
//                .window(SlidingEventTimeWindows.of(Time.milliseconds(2) /* size */, Time.milliseconds(1) /* slide */))
//                .apply(new JoinFunction<Integer, Integer, String>() {
//                        @Override
//                        public String join(Integer first, Integer second) {
//                            return first + "," + second;
//                        }
//                 });
        env.execute(A62SlidingWindowJoin.class.getSimpleName());
    }

}
