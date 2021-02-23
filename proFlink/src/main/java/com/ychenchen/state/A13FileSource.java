package com.ychenchen.state;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author alexis.yang
 * @since 2021/2/23 9:29 AM
 */
public class A13FileSource implements SourceFunction<String> {

    public String filePath;

    public A13FileSource(String filePath) {
        this.filePath = filePath;
    }

    BufferedReader bufferedReader;
//    private InputStream inputStream;

    private Random random = new Random();

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            // 模拟发送数据
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            // 发送数据
            ctx.collect(line);
        }
        if (bufferedReader != null) {
            bufferedReader.close();
        }
//        if (inputStream != null) {
//            inputStream.close();
//        }
    }

    @Override
    public void cancel() {
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
//        if (inputStream != null) {
//            inputStream.close();
//        }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
