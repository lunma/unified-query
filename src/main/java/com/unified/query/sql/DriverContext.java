package com.unified.query.sql;

import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableMap;

/**
 * @author lun.ma
 * @version DriverContext.java, v 0.1 2024-10-21 14:55
 */
public class DriverContext {

    private static final ConcurrentHashMap<String, DataSource> dataSources = new ConcurrentHashMap<>();

    public static void addDataSource(String name, DataSource ds) {
        dataSources.put(name, ds);
    }

    public static ImmutableMap<String,DataSource> getDataSources(){
        return ImmutableMap.copyOf(dataSources);
    }
}
