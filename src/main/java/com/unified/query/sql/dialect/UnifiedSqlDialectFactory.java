package com.unified.query.sql.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.dialect.AccessSqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.apache.calcite.sql.dialect.DerbySqlDialect;
import org.apache.calcite.sql.dialect.ExasolSqlDialect;
import org.apache.calcite.sql.dialect.FirebirdSqlDialect;
import org.apache.calcite.sql.dialect.FireboltSqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.dialect.InfobrightSqlDialect;
import org.apache.calcite.sql.dialect.InformixSqlDialect;
import org.apache.calcite.sql.dialect.IngresSqlDialect;
import org.apache.calcite.sql.dialect.InterbaseSqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.LucidDbSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.NeoviewSqlDialect;
import org.apache.calcite.sql.dialect.NetezzaSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.ParaccelSqlDialect;
import org.apache.calcite.sql.dialect.PhoenixSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.dialect.SybaseSqlDialect;
import org.apache.calcite.sql.dialect.TeradataSqlDialect;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;

/**
 * @author lun.ma
 * @version UnifiedSqlDialectFactory.java, v 0.1 2024-10-16 16:59
 */
public class UnifiedSqlDialectFactory implements SqlDialectFactory {


    private final JethroDataSqlDialect.JethroInfoCache jethroCache =
            JethroDataSqlDialect.createCache();

    @Override
    public SqlDialect
    create(DatabaseMetaData databaseMetaData) {
        SqlDialect.Context c = UnifiedSqlDialects.createContext(databaseMetaData);

        String databaseProductName = c.databaseProductName();
        try {
            if (databaseProductName == null) {
                databaseProductName = databaseMetaData.getDatabaseProductName();
            }
        } catch (SQLException e) {
            throw new RuntimeException("while detecting database product", e);
        }
        final String upperProductName =
                databaseProductName.toUpperCase(Locale.ROOT).trim();
        switch (upperProductName) {
            case "ACCESS":
                return new AccessSqlDialect(c);
            case "APACHE DERBY":
                return new DerbySqlDialect(c);
            case "CLICKHOUSE":
                return new ClickHouseSqlDialect(c);
            case "DBMS:CLOUDSCAPE":
                return new DerbySqlDialect(c);
            case "EXASOL":
                return new ExasolSqlDialect(c);
            case "FIREBOLT":
                return new FireboltSqlDialect(c);
            case "HIVE":
                return new HiveSqlDialect(c);
            case "MAXCOMPUTE/ODPS":
                return new OdpsDialect(c);
            case "INGRES":
                return new IngresSqlDialect(c);
            case "INTERBASE":
                return new InterbaseSqlDialect(c);
            case "JETHRODATA":
                return new JethroDataSqlDialect(
                        c.withJethroInfo(jethroCache.get(databaseMetaData)));
            case "LUCIDDB":
                return new LucidDbSqlDialect(c);
            case "ORACLE":
                return new OracleSqlDialect(c);
            case "PHOENIX":
                return new PhoenixSqlDialect(c);
            case "MYSQL (INFOBRIGHT)":
                return new InfobrightSqlDialect(c);
            case "MYSQL":
                return new MysqlSqlDialect(
                        c.withDataTypeSystem(MysqlSqlDialect.MYSQL_TYPE_SYSTEM));
            case "REDSHIFT":
                return new RedshiftSqlDialect(
                        c.withDataTypeSystem(RedshiftSqlDialect.TYPE_SYSTEM));
            case "SNOWFLAKE":
                return new SnowflakeSqlDialect(c);
            case "SPARK":
                return new SparkSqlDialect(c);
            default:
                break;
        }
        // Now the fuzzy matches.
        if (upperProductName.startsWith("DB2")) {
            return new Db2SqlDialect(c);
        } else if (upperProductName.contains("FIREBIRD")) {
            return new FirebirdSqlDialect(c);
        } else if (upperProductName.contains("FIREBOLT")) {
            return new FireboltSqlDialect(c);
        } else if (upperProductName.contains("GOOGLE BIGQUERY")
                || upperProductName.contains("GOOGLE BIG QUERY")) {
            return new BigQuerySqlDialect(c);
        } else if (upperProductName.startsWith("INFORMIX")) {
            return new InformixSqlDialect(c);
        } else if (upperProductName.contains("NETEZZA")) {
            return new NetezzaSqlDialect(c);
        } else if (upperProductName.contains("PARACCEL")) {
            return new ParaccelSqlDialect(c);
        } else if (upperProductName.startsWith("HP NEOVIEW")) {
            return new NeoviewSqlDialect(c);
        } else if (upperProductName.contains("POSTGRE")) {
            return new PostgresqlSqlDialect(
                    c.withDataTypeSystem(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM));
        } else if (upperProductName.contains("SQL SERVER")) {
            return new MssqlSqlDialect(c);
        } else if (upperProductName.contains("SYBASE")) {
            return new SybaseSqlDialect(c);
        } else if (upperProductName.contains("TERADATA")) {
            return new TeradataSqlDialect(c);
            //处理ODPS
        } else if (upperProductName.contains("HSQL")) {
            return new HsqldbSqlDialect(c);
        } else if (upperProductName.contains("H2")) {
            return new H2SqlDialect(c);
        } else if (upperProductName.contains("VERTICA")) {
            return new VerticaSqlDialect(c);
        } else if (upperProductName.contains("SNOWFLAKE")) {
            return new SnowflakeSqlDialect(c);
        } else if (upperProductName.contains("SPARK")) {
            return new SparkSqlDialect(c);
        } else {
            return new AnsiSqlDialect(c);
        }
    }
}
