package com.unified.query.sql.dialect;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * @author lun.ma
 * @version OdpsDialect.java, v 0.1 2024-10-17 17:00
 */
public class OdpsDialect extends SqlDialect {
    /**
     * Creates a SparkSqlDialect.
     */
    public OdpsDialect(SqlDialect.Context context) {
        super(context);
    }

    @Override protected boolean allowsAs() {
        return false;
    }

    @Override public boolean supportsCharSet() {
        return false;
    }

    @Override public JoinType emulateJoinTypeForCrossJoin() {
        return JoinType.CROSS;
    }

    @Override public boolean supportsGroupByWithRollup() {
        return true;
    }

    @Override public boolean supportsNestedAggregations() {
        return false;
    }

    @Override public boolean supportsApproxCountDistinct() {
        return true;
    }

    @Override public boolean supportsGroupByWithCube() {
        return true;
    }

    @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
                                             @Nullable SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }

}
