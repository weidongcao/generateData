package com.yamu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * 读取resources资源目录下的配置文件，默认为application.properties
 * 获取配置项
 * @author caoweidong
 */
public class ConfigUtil {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

    private static final Properties PROP = new Properties();

    /**
     * 读取的配置文件名，默认为application.properties
     */
    private static final String CONF_NAME = "application.properties";

    static {
        try {
            InputStream in = ConfigUtil.class.getClassLoader().getResourceAsStream(CONF_NAME);

            PROP.load(in);
            logger.info("load config file success --> {}", CONF_NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取指定key对应的value
     * @param key 键
     * @return 值(字符串)
     */
    public static String getString(String key) {
        return PROP.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key 键
     * @return 值(数字)
     */
    public static Integer getInteger(String key) {
        String value = getString(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key 键
     * @return 值(Boolean: true & false）
     */
    public static Boolean getBoolean(String key) {
        String value = getString(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        logger.debug("wedo: {}", getInteger("schedule.seconds"));
    }

}
