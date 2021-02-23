package com.yamu;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 实现Runnable接口
 * 生成日志文件
 * @author wedo
 * @date 2021-02-03 20:04:28
 */
public class GenerateData2Clickhouse implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(GenerateData2Clickhouse.class);

    public static final DateFormat FORMATTER = new SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 每秒日志数据量，默认60万
     */
    public static final int GENERATE_DATA_COUNT = ConfigUtil.getInteger("generate.data.one.file.count");

    /**
     * 模板数据
     */
    public static final String[] DATA_TEMPLATE_ARRAY;

//    public static final String INSERT_TEMPLATE = "insert into src.src_dns_logs_cache(src_ip, domain_name, parse_timestamp, a_record, rcode, qtype, cname, aaaa_record, business_ip) values";
    public static final String INSERT_TEMPLATE = "insert into src.wedotest(src_ip, domain_name, parse_timestamp, a_record, rcode, qtype, cname, aaaa_record, business_ip) values";



    private static final int DATA_UNIT = 100;

    static {
        // 读取resources目录下的dns_temple_data文件
        String dnsTemplateDataStr = null;
        try {
            dnsTemplateDataStr = IOUtils.toString(
                    Objects.requireNonNull(AppGenerateEverySecond.class.getClassLoader().getResourceAsStream("insert_temple_data")),
                    StandardCharsets.UTF_8
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert dnsTemplateDataStr != null;
        dnsTemplateDataStr = dnsTemplateDataStr.trim();
        List<String> list = Arrays.asList(dnsTemplateDataStr.split("\r\n"));
        int dataUnitCount = list.size() / DATA_UNIT;
        if (list.size() % DATA_UNIT != 0) {
            dataUnitCount += 1;
        }
        // 将日志内容字符串转为日志记录数组
        DATA_TEMPLATE_ARRAY = new String[dataUnitCount];
        for (int i = 0; i < dataUnitCount; i++) {
            if (i + 1 == dataUnitCount) {
                DATA_TEMPLATE_ARRAY[i] = StringUtils.join(list.subList(i * DATA_UNIT, list.size()), ",");
            } else {
                DATA_TEMPLATE_ARRAY[i] = StringUtils.join(list.subList(i * DATA_UNIT, (i + 1) * DATA_UNIT), ",");
            }
        }
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        JDBCHelper helper = JDBCHelper.getInstance();

        String timeStr = FORMATTER.format(Calendar.getInstance().getTime());
        //1次写入文件的数据量
        int onceWriteCount = 700000;
        //根据模板数据生成日志文件内容
        List<String> dataList = new ArrayList<>();
        //随机获取模板数据
        Random random = new Random();
        for (int i = 0; i < GENERATE_DATA_COUNT / DATA_UNIT; i++) {
            int a = random.nextInt(DATA_TEMPLATE_ARRAY.length);
            //替换模板数据的时间
            dataList.add(DATA_TEMPLATE_ARRAY[a].replaceAll("\\{time}", timeStr));
            if (dataList.size() * DATA_UNIT - onceWriteCount >= 0) {
                //写入日志文件
                String data = StringUtils.join(dataList, ", ");
                helper.executeUpdateBySql(INSERT_TEMPLATE + data);
                logger.info("success insert into ClickHouse, data count{}, time: {}, consume:{}", dataList.size(), timeStr, (System.currentTimeMillis() - startTime));
                dataList = new ArrayList<>();
            }
        }

        if (dataList.size() > 0) {
            String data = StringUtils.join(dataList, ", ");
            helper.executeUpdateBySql(INSERT_TEMPLATE + data);
            logger.info("success insert into ClickHouse, data count{}, time: {}, consume:{}", GENERATE_DATA_COUNT, timeStr, (System.currentTimeMillis() - startTime));
        }

        logger.info("success!!! insert into ClickHouse, data time: {}, time consume: {}", timeStr, (System.currentTimeMillis() - startTime));
    }

    public static void main(String[] args) {
        new GenerateData2Clickhouse().run();
    }
}
