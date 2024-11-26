package com.unified.query.sql.service;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.io.FileUtils;

import com.unified.query.schema.UnifiedSchemaFactory;

import lombok.extern.slf4j.Slf4j;


/**
 * @author lun.ma
 * @version CalciteService.java, v 0.1 2024-09-20 14:37
 */
@Slf4j
public class CalciteService {

    public static void start() {
        try {
            File loapTmp = UnifiedSchemaFactory.createTempJson();
            Properties properties = new Properties();
            properties.setProperty("quoting", Quoting.DOUBLE_QUOTE.name());
            properties.setProperty("caseSensitive", "false");
            properties.setProperty("quotedCasing", Casing.UNCHANGED.name());
            properties.setProperty("unquotedCasing", Casing.UNCHANGED.name());
            properties.put("model", loapTmp.getAbsolutePath());

            File olapUser = File.createTempFile("olap_user_", ".properties");
            FileUtils.writeLines(olapUser, Arrays.asList("root: jDGkzCjYYjvZnLz4iWpv, user"));

            doStart(properties, olapUser);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void doStart(Properties properties, File userFile) throws SQLException {
        JdbcMeta jdbcMeta = new JdbcMeta("jdbc:calcite:", properties);
        LocalService localService = new LocalService(jdbcMeta);
        AvaticaJsonHandler jsonHandler = new AvaticaJsonHandler(localService);


        new HttpServer.Builder<>()
                .withHandler(jsonHandler)
                .withPort(8765)
                .withBasicAuthentication(userFile.getAbsolutePath(), new String[]{"user"})
                .build()
                .start();

    }

}
