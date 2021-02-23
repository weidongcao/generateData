package com.yamu;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * 实现Runnable接口
 * 生成日志文件
 */
public class GenerateData2File implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(GenerateData2File.class);

    public static final DateFormat FORMATTER = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 生成的日志文件命名格式
     */
    public static final String DATA_FILE_NAME_PATTERN = ConfigUtil.getString("data.file.name.pattern");

    /**
     * 模板数据
     */
    public static final String[] DATA_TEMPLATE_ARRAY;

    public static final int dataUnit = 100;

    static {
        logger.info("begin to generate dns resolve log data");

        String osName = System.getProperty("os.name");

        // 读取resources目录下的dns_temple_data文件
        String dnsTemplateDataStr = null;
        try {
            dnsTemplateDataStr = IOUtils.toString(
                    Objects.requireNonNull(AppGenerateEverySecond.class.getClassLoader().getResourceAsStream("dns_temple_data")),
                    StandardCharsets.UTF_8
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert dnsTemplateDataStr != null;
        dnsTemplateDataStr = dnsTemplateDataStr.trim();
        List<String> list = Arrays.asList(dnsTemplateDataStr.split("\r\n"));
        int dataUnitCount = list.size() / dataUnit;
        if (list.size() % dataUnit != 0) {
            dataUnitCount += 1;
        }
        // 将日志内容字符串转为日志记录数组
        DATA_TEMPLATE_ARRAY = new String[dataUnitCount];
        for (int i = 0; i < dataUnitCount; i++) {
            if (i + 1 == dataUnitCount) {
                DATA_TEMPLATE_ARRAY[i] = StringUtils.join(list.subList(i * dataUnit, list.size()), "\n");
            } else {
                DATA_TEMPLATE_ARRAY[i] = StringUtils.join(list.subList(i * dataUnit, (i + 1) * dataUnit), "\n");
            }
        }
    }

    private String filePath;
    private int fileDataCount;

    public GenerateData2File(String filePath, int fileDataCount) {
        this.filePath = filePath;
        this.fileDataCount = fileDataCount;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        String timeFlag = FORMATTER.format(Calendar.getInstance().getTime());
        //装配文件路径文件名
        int temp = new Random().nextInt(900) + 100;
        String dataFileName = DATA_FILE_NAME_PATTERN.replace("{time}", timeFlag + "_" + temp);

        Calendar calendar = Calendar.getInstance();
        String t = FORMATTER.format(calendar.getTime());
        try {
            //1次写入文件的数据量
            int onceWriteCount = 100000;
            //根据模板数据生成日志文件内容
            List<String> dataList = new ArrayList<>();

            //随机获取模板数据
            Random random = new Random();
            FileOutputStream fos = new FileOutputStream(dataFileName);
            GZIPOutputStream gos = new GZIPOutputStream(fos);
            for (int i = 0; i < fileDataCount / dataUnit; i++) {
                int a = random.nextInt(DATA_TEMPLATE_ARRAY.length);
                //替换模板数据的时间
                dataList.add(DATA_TEMPLATE_ARRAY[a].replaceAll("\\{time}", t));
                if (dataList.size() * dataUnit - onceWriteCount >= 0) {
                    //写入日志文件
                    String data = StringUtils.join(dataList, "\n") + "\n";
                    gos.write(data.getBytes());
                    dataList = new ArrayList<>();
                }
            }

            if (dataList.size() > 0) {
                //写入日志文件
                String data = StringUtils.join(dataList, "\n") + "\n";
                gos.write(data.getBytes());
            }
            gos.close();

            File dataFile = new File(dataFileName);
            File destDir = new File(filePath);
            if (!destDir.exists()) {
                destDir.mkdirs();
            }
            File destFile = new File(filePath + File.separator + dataFileName);
            if (dataFile.renameTo(destFile)) {
                logger.info("generate success!!! filename: {}, time consume: {}", dataFile, (System.currentTimeMillis() - startTime));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
