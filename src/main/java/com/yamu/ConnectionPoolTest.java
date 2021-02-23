package com.yamu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author wedo
 */
public class ConnectionPoolTest {

    public static void main(String[] args) {
        try {
            //单例模式创建连接池对象
            ConnectionPool connPool = ConnectionPoolUtils.getPoolInstance();
            // SQL测试语句
            String sql = "select 1 from src.src_dns_logs_cache limit 3;";
            // 设定程序运行起始时间
            long start = System.currentTimeMillis();
            // 循环测试100次数据库连接
            for (int i = 0; i < 100; i++) {
                // 从连接库中获取一个可用的连接
                Connection conn = connPool.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    String name = rs.getString("name");
                    System.out.println("查询结果" + name);
                }
                rs.close();
                stmt.close();
                // 连接使用完后释放连接到连接池
                connPool.returnConnection(conn);
            }
            System.out.println("经过100次的循环调用，使用连接池花费的时间:" + (System.currentTimeMillis() - start) + "ms");
            // connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
            connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
            // 设定程序运行起始时间
            start = System.currentTimeMillis();

            /*不使用连接池创建100个连接的时间*/
            // 导入驱动
            Class.forName("com.mysql.jdbc.Driver");
            for (int i = 0; i < 100; i++) {
                // 创建连接
                Connection conn = DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/test", "root", "123456");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                }
                rs.close();
                stmt.close();
                conn.close();// 关闭连接
            }
            System.out.println("经过100次的循环调用，不使用连接池花费的时间:"
                    + (System.currentTimeMillis() - start) + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}