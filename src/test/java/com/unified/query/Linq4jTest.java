package com.unified.query;

import java.util.ArrayList;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

/**
 * @author lun.ma
 * @version Linq4jTest.java, v 0.1 2024-10-18 9:21
 */
public class Linq4jTest {

    @Test
    public void testSelectiveDatasource(){

        ArrayList<String> originList = Lists.newArrayList("A", "B", "C");
        ArrayList<String> targetList = Lists.newArrayList("A");
        Linq4j.asEnumerable(originList).intersect(Linq4j.asEnumerable(targetList)).forEach(System.out::println);
        Queryable<String> queryable = Linq4j.asEnumerable(originList).intersect(Linq4j.asEnumerable(targetList)).asQueryable();
        Expression expression = queryable.getExpression();
        System.out.println(expression);
    }

}
