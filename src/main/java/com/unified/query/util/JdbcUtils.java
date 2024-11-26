package com.unified.query.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

/**
 * 内置数据库
 *
 * @author lun.ma
 * @version jdbcUtils.java, v 0.1 2024-10-18 15:50
 */
public class JdbcUtils {

    private static final String JDBC_MEMORY_URL = "jdbc:h2:mem:data";
    private static final String JDBC_FILE_URL = "jdbc:h2:file:./h2/data;AUTO_SERVER=TRUE";
    private static final String JDBC_TCP_URL = "jdbc:h2:tcp://localhost:19200/./h2/data";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "cWjCcxrj0F4Ug1ufeZLM";

    @SneakyThrows
    public static Connection getConnection() {
        return DriverManager.getConnection(JDBC_FILE_URL, USERNAME, PASSWORD);
    }

    private static final String CREATE_TABLE = "create table if not exists datasource_management( \n" +
            "id int primary key auto_increment, \n" +
            "schema_name varchar(255), \n" +
            "driver varchar(255), \n" +
            "jdbc_url varchar(255), \n" +
            "username varchar(255), \n" +
            "password varchar(255),\n" +
            "unique(schema_name,driver)\n" +
            ")";
    private static final String DROP_TABLE = "drop table datasource_management";

    private static final String QUERY = "select id,schema_name,driver,jdbc_url,username,password from datasource_management";
    private static final String INSERT = "insert into datasource_management(schema_name,driver,jdbc_url,username,password) values(?,?,?,?,?)";


    public boolean checkTableExists(String tableName) {
        //TODO
        return true;
    }

    public static boolean createTable() {
        try (Connection connection = JdbcUtils.getConnection();
             Statement statement = connection.createStatement();) {
            return statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean dropTable() {
        try (Connection connection = JdbcUtils.getConnection();
             Statement statement = connection.createStatement();) {
            return statement.execute(DROP_TABLE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<InnerDatasource> queryStatement() {
        try (Connection connection = JdbcUtils.getConnection();
             Statement statement = connection.createStatement();) {
            ResultSet resultSet = statement.executeQuery(QUERY);

            List<InnerDatasource> datasources = new ArrayList<>();
            while (resultSet.next()) {
                datasources.add(
                        new InnerDatasource()
                                .setId(resultSet.getInt("id"))
                                .setSchemaName(resultSet.getString("schema_name"))
                                .setDriver(resultSet.getString("driver"))
                                .setJdbcUrl(resultSet.getString("jdbc_url"))
                                .setUsername(resultSet.getString("username"))
                                .setPassword(resultSet.getString("password")));
            }
            return datasources;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static int[] insertBatchStatement(List<InnerDatasource> datasources) {
        try (Connection connection = JdbcUtils.getConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT)) {
            for (int i = 0; i < datasources.size(); i++) {
                statement.setString(1, datasources.get(i).getSchemaName());
                statement.setString(2, datasources.get(i).getDriver());
                statement.setString(3, datasources.get(i).getJdbcUrl());
                statement.setString(4, datasources.get(i).getUsername());
                statement.setString(5, datasources.get(i).getPassword());
                statement.addBatch();
            }
            return statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Data
    @EqualsAndHashCode
    @Accessors(chain = true)
    public static class InnerDatasource {
        private int id;
        private String schemaName;
        private String jdbcUrl;
        private String driver;
        private String username;
        private String password;
    }

}
