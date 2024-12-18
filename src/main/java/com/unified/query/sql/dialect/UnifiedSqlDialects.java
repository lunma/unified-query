package com.unified.query.sql.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;

/**
 * @author lun.ma
 * @version UnifiedSqlDialects.java, v 0.1 2024-10-16 17:04
 */
public final class UnifiedSqlDialects {

    public static SqlDialect.Context createContext(DatabaseMetaData databaseMetaData) {
        String databaseProductName;
        int databaseMajorVersion;
        int databaseMinorVersion;
        String databaseVersion;
        try {
            databaseProductName = databaseMetaData.getDatabaseProductName();
            databaseMajorVersion = databaseMetaData.getDatabaseMajorVersion();
            databaseMinorVersion = databaseMetaData.getDatabaseMinorVersion();
            databaseVersion = databaseMetaData.getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new RuntimeException("while detecting database product", e);
        }
        final String quoteString = getIdentifierQuoteString(databaseMetaData);
        final NullCollation nullCollation = getNullCollation(databaseMetaData);
        final Casing unquotedCasing = getCasing(databaseMetaData, false);
        final Casing quotedCasing = getCasing(databaseMetaData, true);
        final boolean caseSensitive = isCaseSensitive(databaseMetaData);
        final SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT
                .withDatabaseProductName(databaseProductName)
                .withDatabaseMajorVersion(databaseMajorVersion)
                .withDatabaseMinorVersion(databaseMinorVersion)
                .withDatabaseVersion(databaseVersion)
                .withIdentifierQuoteString(quoteString)
                .withUnquotedCasing(unquotedCasing)
                .withQuotedCasing(quotedCasing)
                .withCaseSensitive(caseSensitive)
                .withNullCollation(nullCollation);

        return c;
    }

    private static String getIdentifierQuoteString(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.getIdentifierQuoteString();
        } catch (SQLException e) {
            throw new IllegalArgumentException("cannot deduce identifier quote string", e);
        }
    }

    private static Casing getCasing(DatabaseMetaData databaseMetaData, boolean quoted) {
        try {

            //处理ODPS
            if(isOdps(databaseMetaData)){
                return Casing.UNCHANGED;
            }

            if (quoted
                    ? databaseMetaData.storesUpperCaseQuotedIdentifiers()
                    : databaseMetaData.storesUpperCaseIdentifiers()) {
                return Casing.TO_UPPER;
            } else if (quoted
                    ? databaseMetaData.storesLowerCaseQuotedIdentifiers()
                    : databaseMetaData.storesLowerCaseIdentifiers()) {
                return Casing.TO_LOWER;
            } else if (quoted
                    ? (databaseMetaData.storesMixedCaseQuotedIdentifiers()
                    || databaseMetaData.supportsMixedCaseQuotedIdentifiers())
                    : (databaseMetaData.storesMixedCaseIdentifiers()
                    || databaseMetaData.supportsMixedCaseIdentifiers())) {
                return Casing.UNCHANGED;
            } else {
                return Casing.UNCHANGED;
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException("cannot deduce casing", e);
        }
    }

    private static boolean isCaseSensitive(DatabaseMetaData databaseMetaData) {
        try {
            //处理ODPS
            if(isOdps(databaseMetaData)){
                return false;
            }
            return databaseMetaData.supportsMixedCaseIdentifiers()
                    || databaseMetaData.supportsMixedCaseQuotedIdentifiers();
        } catch (SQLException e) {
            throw new IllegalArgumentException("cannot deduce case-sensitivity", e);
        }
    }

    private static NullCollation getNullCollation(DatabaseMetaData databaseMetaData) {
        try {
            //处理ODPS
            if (isOdps(databaseMetaData)) {
                return NullCollation.LOW;
            } else if (databaseMetaData.nullsAreSortedAtEnd()) {
                return NullCollation.LAST;
            } else if (databaseMetaData.nullsAreSortedAtStart()) {
                return NullCollation.FIRST;
            } else if (databaseMetaData.nullsAreSortedLow()) {
                return NullCollation.LOW;
            } else if (databaseMetaData.nullsAreSortedHigh()) {
                return NullCollation.HIGH;
            } else if (isBigQuery(databaseMetaData)) {
                return NullCollation.LOW;
            } else {
                throw new IllegalArgumentException("cannot deduce null collation");
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException("cannot deduce null collation", e);
        }
    }

    private static boolean isBigQuery(DatabaseMetaData databaseMetaData)
            throws SQLException {
        return databaseMetaData.getDatabaseProductName()
                .equals("Google Big Query");
    }

    private static boolean isOdps(DatabaseMetaData databaseMetaData)
            throws SQLException {
        return databaseMetaData.getDatabaseProductName()
                .equals("MaxCompute/ODPS");
    }
}
