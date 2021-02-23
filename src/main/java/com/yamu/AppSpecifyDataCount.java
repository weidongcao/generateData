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
public class AppSpecifyDataCount {
    /** 打印日志 */
    private static final Logger logger = LoggerFactory.getLogger(AppSpecifyDataCount.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        String path = "data";
        int fileDataCount = 60 * 10000;
        if (args.length >= 2) {
            path = args[0];
            fileDataCount = Integer.parseInt(args[1]) * 10000;
        }
        logger.info("generate data path: {}", path);
        logger.info("generate one file data count: {}", fileDataCount);

        int dataCount = 3000 * 10000;
        if (args.length >= 3) {
            dataCount = Integer.parseInt(args[2]) * 10000;
        }

        logger.info("program will generate {} data", dataCount);
        int threadNum = dataCount / fileDataCount;
        if (dataCount % fileDataCount != 0) {
            threadNum += 1;
        }
        long l1 = System.currentTimeMillis();
        // 创建线程池每秒生成1个日志文件
        ExecutorService service = Executors.newFixedThreadPool(8);
        for (int i = 0; i < threadNum; i++) {
            service.execute(new GenerateData2File(path, fileDataCount));
        }
        long l2 = System.currentTimeMillis();
        System.out.println((l2 - l1) / 1000);
        service.shutdown();
    }

}

