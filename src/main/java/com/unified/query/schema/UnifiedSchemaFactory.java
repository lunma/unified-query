package com.unified.query.schema;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.unified.query.sql.DriverContext;
import com.unified.query.sql.dialect.UnifiedSqlDialectFactory;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lun.ma
 * @version UnifiedSchemaFactory.java, v 0.1 2024-10-17 10:23
 */

@Slf4j
public class UnifiedSchemaFactory implements SchemaFactory {
    private static final Map<String, File> cachedJsons = Maps.newConcurrentMap();

    @SneakyThrows
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        UnifiedSqlDialectFactory dialectFactory = new UnifiedSqlDialectFactory();

        ImmutableMap<String, DataSource> sources = DriverContext.getDataSources();
        for (Map.Entry<String, DataSource> entry : sources.entrySet()) {
            DataSource dataSource = entry.getValue();
            log.info("model.json load schema:{},config file schema:{}", name, entry.getKey());
            if (StringUtils.equalsIgnoreCase(name, entry.getKey())) {
                try (Connection connection = dataSource.getConnection()) {
                    return JdbcSchema.create(parentSchema,
                            name,
                            dataSource,
                            dialectFactory,
                            connection.getCatalog() == null ? connection.getSchema() : connection.getCatalog(),
                            connection.getSchema() == null ? null : connection.getSchema());
                }
            }
        }
        return null;
    }

    public static File createTempJson() {
        try {
            StringBuilder out = new StringBuilder();
            out.append("{\n");
            out.append("    \"version\": \"1.0\",\n");
            out.append("    \"schemas\": [\n");

            int counter = 0;
            ImmutableMap<String, DataSource> configSchema = DriverContext.getDataSources();
            for (String schemaName : configSchema.keySet()) {
                out.append("        {\n");
                out.append("            \"type\": \"custom\",\n");
                out.append("            \"name\": \"" + schemaName + "\",\n");
                out.append("            \"factory\": \"com.unified.query.schema.UnifiedSchemaFactory\"\n");
                out.append("        }\n");

                if (++counter != configSchema.size()) {
                    out.append(",\n");
                }
            }
            out.append("    ]\n");
            out.append("}\n");

            String jsonContent = out.toString();
            File file = cachedJsons.get(jsonContent);
            if (file == null) {
                file = File.createTempFile("olap_model_", ".json");
                file.deleteOnExit();
                FileUtils.writeStringToFile(file, jsonContent);

                log.debug("Adding new schema file {} to cache", file.getName());
                log.info("Schema json: " + jsonContent);
                cachedJsons.put(jsonContent, file);
            }
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
