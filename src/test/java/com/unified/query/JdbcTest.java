package com.unified.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.junit.jupiter.api.Test;

import com.unified.query.sql.ResultSetPrinter;

/**
 * @author lun.ma
 * @version JdbcTest.java, v 0.1 2024-10-17 9:48
 */
public class JdbcTest {

    @Test
    public void testJdbc() {
        try {
            // 加载 Avatica 驱动
            Class.forName("org.apache.calcite.avatica.remote.Driver");

            // 连接到 Avatica HTTP 服务器
            String url = "jdbc:avatica:remote:url=http://localhost:8765";
            Properties config = new Properties();
            config.setProperty(BuiltInConnectionProperty.SERIALIZATION.camelName(), "json");
            config.setProperty("avatica_user", "root");
            config.setProperty("avatica_password", "jDGkzCjYYjvZnLz4iWpv");
            config.setProperty(BuiltInConnectionProperty.AUTHENTICATION.camelName(), AuthenticationType.BASIC.name());
            Connection conn = DriverManager.getConnection(url, config);

            // 创建 Statement 对象
            Statement stmt = conn.createStatement();

            // 执行 SQL 查询
//            String sql = "SELECT id FROM sqa.`user` where id = 2";
            //        String sql = "SELECT * FROM sqa.rule UNION ALL SELECT * FROM architecture_standard.rule";
//        String sql = "SELECT t1.* FROM sqa.rule as t1 left join architecture_standard.rule as t2 on t1.id = t2.id";
        String sql = "SELECT id FROM sqa.\"user\" where id = 2";
//            String sql = "select * from cicdata_meta_dev.all_project_task_runtime_dqc where dt = '20240401' limit 1";
/*        String sql = "select odps_project_name from cicdata_meta_dev.all_project_task_runtime_dqc where dt = '20240401' " +
                "union all " +
                "SELECT id FROM sqa.`user` where id = 2";*/
            ResultSet rs = stmt.executeQuery(sql);

            ResultSetPrinter.printResultSet(rs);

            // 关闭资源
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
