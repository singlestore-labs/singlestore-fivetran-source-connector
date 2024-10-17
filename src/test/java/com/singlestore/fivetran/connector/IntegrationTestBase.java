package com.singlestore.fivetran.connector;

import com.google.common.collect.ImmutableMap;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;

public class IntegrationTestBase {

  static String host = "127.0.0.1";
  static String port = "3306";
  static String user = "root";
  static String password = System.getenv("ROOT_PASSWORD");
  static String database = "db";

  static SingleStoreConfiguration getConfig(String table) {
    return new SingleStoreConfiguration(ImmutableMap.of("host", host,
        "port", port, "user", user, "password", password, "database", database, "table", table));
  }

  @BeforeAll
  public static void init() throws Exception {
    SingleStoreConfiguration conf = getConfig("init");
    SingleStoreConnection conn = new SingleStoreConnection(conf);
    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("SET GLOBAL enable_observe_queries=1");
    }
  }
}
