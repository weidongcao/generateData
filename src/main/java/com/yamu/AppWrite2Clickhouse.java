package com.yamu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 需求说明：
 * resources目录下的dns_temple_data文件有20万生产环境模板数据，只替换了时间
 * 1. 读取配置文件
 * 2. 读取模板数据
 * 3. 创建线程，每秒随机生成指定数据的数据（60万）
 * 4. 随机生成的dns解析日志写入日志文件，文件名格式为100_1_{time}_1.gz,eg:100_1_20210130192038731_1.gz
 *
 * @date 2021-01-30 19:31:19
 * @author wedo
 */
public class AppWrite2Clickhouse {
    /** 打印日志 */
    private static final Logger logger = LoggerFactory.getLogger(AppWrite2Clickhouse.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        // 创建线程池每秒生成1个日志文件
        ExecutorService service = Executors.newFixedThreadPool(10);
        service.execute(new GenerateData2Clickhouse());
//        while (true) {
//            service.execute(new GenerateData2Clickhouse());
//            Thread.sleep(1000);
//        }
        service.shutdown();

    }

}

