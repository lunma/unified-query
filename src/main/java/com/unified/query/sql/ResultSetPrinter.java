package com.unified.query.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author lun.ma
 * @version ResultSetPrinter.java, v 0.1 2024-09-26 10:54
 */
public class ResultSetPrinter {
    /**
     * 格式化打印 ResultSet 的内容。
     *
     * @param resultSet 要打印的 ResultSet
     * @throws SQLException 如果 ResultSet 操作失败
     */
    public static void printResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            System.out.println("ResultSet is null.");
            return;
        }

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 打印表头
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            header.append(String.format("%-20s", metaData.getColumnName(i)));
        }
        System.out.println(header.toString());

        // 打印分割线
        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            separator.append("------------------------------");
        }
        System.out.println(separator.toString());

        // 打印每一行数据
        while (resultSet.next()) {
            StringBuilder row = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                row.append(String.format("%-20s", resultSet.getString(i)));
            }
            System.out.println(row.toString());
        }
    }
}
