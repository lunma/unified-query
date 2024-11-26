package com.unified.query.sql.service;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

/**
 * @author lun.ma
 * @version PlanServcie.java, v 0.1 2024-10-21 17:24
 */
public class PlanService {

    public void logicalPlan() throws Exception {
        FrameworkConfig config = Frameworks.newConfigBuilder().build();

        SqlParser sqlParser = SqlParser.create("", SqlParser.config());
        SqlNode sqlNode = sqlParser.parseQuery();

        Planner planner = Frameworks.getPlanner(config);
        RelRoot rel = planner.rel(sqlNode);
        System.out.println("Logical Plan:");
        RelOptUtil.toString(rel.rel);
    }

}
