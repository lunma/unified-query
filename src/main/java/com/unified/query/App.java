package com.unified.query;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.google.gson.Gson;
import com.unified.query.util.JdbcUtils;
import com.unified.query.sql.service.CalciteService;
import com.unified.query.sql.DriverContext;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lun.ma
 * @version Application.java, v 0.1 2024-09-20 14:35
 */
@Slf4j
public class App {
    public static void main(String[] args) {
       buildJdbc();
       startSever();
    }

    private static void buildJdbc() {
        boolean notExists = Files.notExists(Paths.get(System.getProperty("user.dir"), "h2"));
        if(notExists) {
            log.info("create table ....");
            JdbcUtils.createTable();
        }

        List<JdbcUtils.InnerDatasource> datasources = JdbcUtils.queryStatement();
        log.info("data sources list : {}",new Gson().toJson(datasources));

        for (JdbcUtils.InnerDatasource datasource : datasources) {
            HikariDataSource hikariDataSource = new HikariDataSource();
            hikariDataSource.setDriverClassName(datasource.getDriver());
            hikariDataSource.setJdbcUrl(datasource.getJdbcUrl());
            hikariDataSource.setUsername(datasource.getUsername());
            hikariDataSource.setPassword(datasource.getPassword());
            DriverContext.addDataSource(datasource.getSchemaName(),hikariDataSource);
        }

    }

    private static void startSever() {
        CalciteService.start();
    }
}
