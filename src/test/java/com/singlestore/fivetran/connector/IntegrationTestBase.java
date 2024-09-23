package com.singlestore.fivetran.connector;

import com.google.common.collect.ImmutableMap;

public class IntegrationTestBase {
  static String host = "127.0.0.1";
  static String port = "3306";
  static String user = "root";
  static String password = System.getenv("ROOT_PASSWORD");
  static String database = "db";
}
