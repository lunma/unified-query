package com.unified.query.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

/**
 * @author lun.ma
 * @version JdbcUtilsTest.java, v 0.1 2024-10-18 17:27
 */
class JdbcUtilsTest {

    @Test
    public void testConnection() throws SQLException {
        Connection connection = JdbcUtils.getConnection();
        assertNotNull(connection);
        connection.close();
        boolean table = JdbcUtils.createTable();
        assertFalse(table);
    }

    @Test
    public void testInsert() {
        ArrayList<JdbcUtils.InnerDatasource> list = new ArrayList<>();
        list.add(new JdbcUtils.InnerDatasource()
                .setUsername("testUsername")
                .setPassword("testPassword")
                .setDriver("com.xxx.Driver")
                .setJdbcUrl("testUrl")
                .setSchemaName("testSchema"));
        int[] ints = JdbcUtils.insertBatchStatement(list);
        assertArrayEquals(new int[]{1}, ints);
    }


    @Test
    public void testQuery() {
        List<JdbcUtils.InnerDatasource> innerDatasources = JdbcUtils.queryStatement();
        assertNotNull(innerDatasources);
        System.out.println(new Gson().toJson(innerDatasources));
    }
}