package com.yamu;

import java.sql.*;

public class JDBCTest {
    private static final String DRIVER = ConfigUtil.getString("clickhouse.driver");
    private static final String URL = ConfigUtil.getString("clickhouse.url");
    private static final String USERNAME = ConfigUtil.getString("clickhouse.username");
    private static final String PASSWORD = ConfigUtil.getString("clickhouse.password");

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(DRIVER);
        Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement pstmt = conn.prepareStatement("select count(1) from default .wedotest");

        ResultSet rs = pstmt.executeQuery();
        rs.next();
        System.out.println(rs.getString(1));

    }
}
