package com.yamu;

/**
 * 连接池工具类，返回唯一的一个数据库连接池对象,单例模式
 *
 * @author wedo
 * @date 2021-02-03 13:57:54
 */
public class ConnectionPoolUtils {
    private ConnectionPoolUtils(){};//私有静态方法
    private static ConnectionPool poolInstance = null;
    public static ConnectionPool getPoolInstance(){
        if(poolInstance == null) {
            poolInstance = new ConnectionPool(
                    ConfigUtil.getString("clickhouse.driver"),
                    ConfigUtil.getString("clickhouse.url"),
                    ConfigUtil.getString("clickhouse.username"),
                    ConfigUtil.getString("clickhouse.password"));
            try {
                poolInstance.createPool();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return poolInstance;
    }
}