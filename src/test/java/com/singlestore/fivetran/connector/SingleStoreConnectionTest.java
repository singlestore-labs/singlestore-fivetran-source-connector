package com.singlestore.fivetran.connector;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import fivetran_sdk.Column;
import fivetran_sdk.DataType;
import fivetran_sdk.DecimalParams;
import fivetran_sdk.Schema;
import fivetran_sdk.SchemaList;
import fivetran_sdk.Table;
import fivetran_sdk.ValueType;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

public class SingleStoreConnectionTest extends IntegrationTestBase {

  @Test
  public void driverParameters() throws Exception {
    SingleStoreConfiguration conf = new SingleStoreConfiguration(ImmutableMap.of("host", host,
        "port", port, "user", user, "password", password, "database", database, "table",
        "driverParameters",
        "driver.parameters",
        "cachePrepStmts = TRUE; allowMultiQueries=  TRUE ;connectTimeout = 20000"));
    SingleStoreConnection conn = new SingleStoreConnection(conf);
    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.executeQuery("SELECT 1; SELECT 2");
    }
  }

  @Test
  public void checkConnection() throws Exception {
    SingleStoreConfiguration conf = getConfig("checkConnection");
    SingleStoreConnection conn = new SingleStoreConnection(conf);
    conn.checkConnection();
  }

  @Test
  public void checkConnectionFailure() {
    SingleStoreConfiguration conf = new SingleStoreConfiguration(ImmutableMap.of("host", host,
        "port", port, "user", "wrongUser", "password", password, "database", database, "table",
        "checkConnectionFailure"));
    SingleStoreConnection conn = new SingleStoreConnection(conf);
    Assertions.assertThrows(SQLException.class, conn::checkConnection);
  }

  @Test
  public void checkTableExistenceTrue() throws Exception {
    SingleStoreConfiguration conf = getConfig("checkTableExistenceTrue");
    SingleStoreConnection conn = new SingleStoreConnection(conf);
    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("CREATE TABLE IF NOT EXISTS checkTableExistenceTrue(a INT)");
    }

    conn.checkTableExistence();
  }

  @Test
  public void checkTableExistenceFalse() throws Exception {
    SingleStoreConfiguration conf = getConfig("checkTableExistenceFalse");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    Assertions.assertThrows(SQLException.class, conn::checkTableExistence);
  }

  @Test
  public void getSchema() throws Exception {
    SingleStoreConfiguration conf = getConfig("getSchema");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS getSchema");
      stmt.execute("CREATE ROWSTORE TABLE IF NOT EXISTS getSchema (\n"
          + "    `boolColumn` BOOL DEFAULT true,\n"
          + "    `booleanColumn` BOOLEAN DEFAULT true,\n"
          + "    `bitColumn` BIT(64) DEFAULT '01234567',\n"
          + "    `tinyintColumn` TINYINT DEFAULT 124,\n"
          + "    `tinyint4Column` TINYINT(4) DEFAULT 124,\n"
          + "    `mediumintColumn` MEDIUMINT DEFAULT 8388607,\n"
          + "    `smallintColumn` SMALLINT DEFAULT 32767,\n"
          + "    `intColumn` INT DEFAULT 2147483647,\n"
          + "    `integerColumn` INTEGER DEFAULT 2147483647,\n"
          + "    `bigintColumn` BIGINT DEFAULT 9223372036854775807,\n"
          + "    `floatColumn` FLOAT DEFAULT 10.1,\n"
          + "    `doubleColumn` DOUBLE DEFAULT 100.1,\n"
          + "    `realColumn` REAL DEFAULT 100.1,\n"
          + "    `dateColumn` DATE DEFAULT '2000-10-10',\n"
          + "    `timeColumn` TIME DEFAULT '22:59:59',\n"
          + "    `time6Column` TIME(6) DEFAULT '22:59:59.111111',\n"
          + "    `datetimeColumn` DATETIME DEFAULT '2023-12-31 23:59:59',\n"
          + "    `datetime6Column` DATETIME(6) DEFAULT '2023-12-31 22:59:59.111111',\n"
          + "    `timestampColumn` TIMESTAMP DEFAULT '2022-01-19 03:14:07',\n"
          + "    `timestamp6Column` TIMESTAMP(6) DEFAULT '2022-01-19 03:14:07.111111',\n"
          + "    `yearColumn` YEAR DEFAULT '1989',\n"
          + "    `decimalColumn` DECIMAL(65, 30) DEFAULT 10000.100001,\n"
          + "    `decColumn` DEC DEFAULT 10000,\n"
          + "    `fixedColumn` FIXED DEFAULT 10000,\n"
          + "    `numericColumn` NUMERIC DEFAULT 10000,\n"
          + "    `charColumn` CHAR DEFAULT 'a',\n"
          + "    `mediumtextColumn` MEDIUMTEXT DEFAULT 'abc',\n"
          + "    `binaryColumn` BINARY DEFAULT 'a',\n"
          + "    `varcharColumn` VARCHAR(100) DEFAULT 'abc',\n"
          + "    `varbinaryColumn` VARBINARY(100) DEFAULT 'abc',\n"
          + "    `longtextColumn` LONGTEXT DEFAULT 'abc',\n"
          + "    `textColumn` TEXT DEFAULT 'abc',\n"
          + "    `tinytextColumn` TINYTEXT DEFAULT 'abc',\n"
          + "    `longblobColumn` LONGBLOB DEFAULT 'abc',\n"
          + "    `mediumblobColumn` MEDIUMBLOB DEFAULT 'abc',\n"
          + "    `blobColumn` BLOB DEFAULT 'abc',\n"
          + "    `tinyblobColumn` TINYBLOB DEFAULT 'abc',\n"
          + "    `jsonColumn` JSON DEFAULT '{}',\n"
          + "    `enum_f` ENUM('val1','val2','val3') default 'val1',\n"
          + "    `set_f` SET('v1','v2','v3') default 'v1',\n"
          + "    `geographyColumn` GEOGRAPHY DEFAULT 'POLYGON((1 1,2 1,2 2, 1 2, 1 1))',\n"
          + "    `geographypointColumn` GEOGRAPHYPOINT DEFAULT 'POINT(1.50000003 1.50000000)',\n"
          + "    `vectorColumn` VECTOR(2, I32) DEFAULT '[1, 2]',\n"
          + "    `bsonColumn` BSON,\n"
          + "     unique key(intColumn),\n"
          + "     shard key(intColumn)\n"
          + " );");
    }

    SchemaList schemaList = conn.getSchema();
    List<Schema> schemas = schemaList.getSchemasList();
    assertEquals(1, schemas.size());

    Schema schema = schemas.get(0);
    assertEquals(database, schema.getName());

    List<Table> tables = schema.getTablesList();
    assertEquals(1, tables.size());

    Table table = tables.get(0);
    assertEquals("getSchema", table.getName());

    List<Column> columns = table.getColumnsList();
    assertEquals(44, columns.size());

    List<String> columnNames = Arrays.asList(
        "boolColumn",
        "booleanColumn",
        "bitColumn",
        "tinyintColumn",
        "tinyint4Column",
        "mediumintColumn",
        "smallintColumn",
        "intColumn",
        "integerColumn",
        "bigintColumn",
        "floatColumn",
        "doubleColumn",
        "realColumn",
        "dateColumn",
        "timeColumn",
        "time6Column",
        "datetimeColumn",
        "datetime6Column",
        "timestampColumn",
        "timestamp6Column",
        "yearColumn",
        "decimalColumn",
        "decColumn",
        "fixedColumn",
        "numericColumn",
        "charColumn",
        "mediumtextColumn",
        "binaryColumn",
        "varcharColumn",
        "varbinaryColumn",
        "longtextColumn",
        "textColumn",
        "tinytextColumn",
        "longblobColumn",
        "mediumblobColumn",
        "blobColumn",
        "tinyblobColumn",
        "jsonColumn",
        "enum_f",
        "set_f",
        "geographyColumn",
        "geographypointColumn",
        "vectorColumn",
        "bsonColumn"
    );

    List<DecimalParams> decimalParameters = Arrays.asList(
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder()
            .setScale(30)
            .setPrecision(65)
            .build(),
        DecimalParams.newBuilder()
            .setScale(0)
            .setPrecision(10)
            .build(),
        DecimalParams.newBuilder()
            .setScale(0)
            .setPrecision(10)
            .build(),
        DecimalParams.newBuilder()
            .setScale(0)
            .setPrecision(10)
            .build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build(),
        DecimalParams.newBuilder().build()
    );

    List<DataType> types = Arrays.asList(
        DataType.SHORT,
        DataType.SHORT,
        DataType.BINARY,
        DataType.SHORT,
        DataType.SHORT,
        DataType.INT,
        DataType.SHORT,
        DataType.INT,
        DataType.INT,
        DataType.LONG,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.DOUBLE,
        DataType.NAIVE_DATE,
        DataType.NAIVE_DATETIME,
        DataType.NAIVE_DATETIME,
        DataType.NAIVE_DATETIME,
        DataType.NAIVE_DATETIME,
        DataType.NAIVE_DATETIME,
        DataType.NAIVE_DATETIME,
        DataType.NAIVE_DATE,
        DataType.DECIMAL,
        DataType.DECIMAL,
        DataType.DECIMAL,
        DataType.DECIMAL,
        DataType.STRING,
        DataType.STRING,
        DataType.BINARY,
        DataType.STRING,
        DataType.BINARY,
        DataType.STRING,
        DataType.STRING,
        DataType.STRING,
        DataType.BINARY,
        DataType.BINARY,
        DataType.BINARY,
        DataType.BINARY,
        DataType.JSON,
        DataType.STRING,
        DataType.STRING,
        DataType.STRING,
        DataType.STRING,
        DataType.STRING,
        DataType.BINARY
    );

    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      assertEquals(columnNames.get(i), column.getName());
      assertEquals(decimalParameters.get(i), column.getDecimal());
      assertEquals(types.get(i), column.getType());
      assertFalse(column.getPrimaryKey());
    }
  }

  @Test
  public void getSchemaPK() throws Exception {
    SingleStoreConfiguration conf = getConfig("getSchemaPK");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS getSchemaPK");
      stmt.execute("CREATE TABLE getSchemaPK (a INT, b INT, PRIMARY KEY(a));");
    }

    SchemaList schemaList = conn.getSchema();
    List<Schema> schemas = schemaList.getSchemasList();
    assertEquals(1, schemas.size());

    Schema schema = schemas.get(0);
    assertEquals(database, schema.getName());

    List<Table> tables = schema.getTablesList();
    assertEquals(1, tables.size());

    Table table = tables.get(0);
    assertEquals("getSchemaPK", table.getName());

    List<Column> columns = table.getColumnsList();
    assertEquals(2, columns.size());
    assertTrue(columns.get(0).getPrimaryKey());
    assertFalse(columns.get(1).getPrimaryKey());
  }

  @Test
  public void getSchemaPKMultiColumn() throws Exception {
    SingleStoreConfiguration conf = getConfig("getSchemaPKMultiColumn");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS getSchemaPKMultiColumn");
      stmt.execute(
          "CREATE TABLE getSchemaPKMultiColumn (a INT, b INT, c INT, d INT, PRIMARY KEY(a, c, d));");
    }

    SchemaList schemaList = conn.getSchema();
    List<Schema> schemas = schemaList.getSchemasList();
    assertEquals(1, schemas.size());

    Schema schema = schemas.get(0);
    assertEquals(database, schema.getName());

    List<Table> tables = schema.getTablesList();
    assertEquals(1, tables.size());

    Table table = tables.get(0);
    assertEquals("getSchemaPKMultiColumn", table.getName());

    List<Column> columns = table.getColumnsList();
    assertEquals(4, columns.size());
    assertTrue(columns.get(0).getPrimaryKey());
    assertFalse(columns.get(1).getPrimaryKey());
    assertTrue(columns.get(2).getPrimaryKey());
    assertTrue(columns.get(3).getPrimaryKey());
  }

  @Test
  public void getNumPartitions() throws Exception {
    SingleStoreConfiguration conf = getConfig("getNumPartitions");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    assertEquals(8, conn.getNumPartitions());
  }

  static class Record {

    String operation;
    Map<String, ValueType> row;

    public Record(String operation, Map<String, ValueType> row) {
      this.operation = operation;
      this.row = row;
    }
  }

  @Test
  public void observe() throws Exception {
    SingleStoreConfiguration conf = getConfig("observe");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS observe");
      stmt.execute(
          "CREATE TABLE observe (a INT, b INT, PRIMARY KEY(a));");
    }

    final Exception[] observeException = new Exception[1];
    SingleStoreConnection observeConn = new SingleStoreConnection(conf);
    List<Record> records = new ArrayList<>();
    State state = new State(8);
    Thread t = new Thread(() -> {
      try {
        observeConn.observe(state, (operation, partition, offset, row) -> {
          if (operation.equals("Delete") || operation.equals("Update") || operation.equals(
              "Insert")) {
            records.add(new Record(operation, row));
            state.setOffset(partition, offset);
          }
        });
      } catch (Exception e) {
        observeException[0] = e;
      }
    });
    t.start();

    try (Statement stmt = conn.getConnection().createStatement()) {
      for (int i = 0; i < 10; i++) {
        stmt.execute(String.format("INSERT INTO observe VALUES(%d, 1)", i));
      }

      stmt.execute("UPDATE observe SET b = 2");

      for (int i = 0; i < 10; i++) {
        stmt.execute(String.format("DELETE FROM observe WHERE a = %d", i));
      }
    }

    Thread.sleep(1000);
    ((com.singlestore.jdbc.Connection) observeConn.getConnection()).cancelCurrentQuery();
    Thread.sleep(1000);
    t.interrupt();

    assertTrue(observeException[0].getMessage().contains("Query execution was interrupted"));

    records.sort((r1, r2) -> {
      if (!r1.operation.equals(r2.operation)) {
        return r1.operation.compareTo(r2.operation);
      } else {
        return Integer.compare(r1.row.get("a").getInt(), r2.row.get("a").getInt());
      }
    });

    for (int i = 0; i < 10; i++) {
      assertEquals("Delete", records.get(i).operation);
      assertEquals((Integer) i, records.get(i).row.get("a").getInt());
      assertNull(records.get(i).row.get("b"));
    }

    for (int i = 10; i < 20; i++) {
      assertEquals("Insert", records.get(i).operation);
      assertEquals((Integer) i - 10, records.get(i).row.get("a").getInt());
      assertEquals((Integer) 1, records.get(i).row.get("b").getInt());
    }

    for (int i = 20; i < 30; i++) {
      assertEquals("Update", records.get(i).operation);
      assertEquals((Integer) i - 20, records.get(i).row.get("a").getInt());
      assertEquals((Integer) 2, records.get(i).row.get("b").getInt());
    }

    records.clear();
    t = new Thread(() -> {
      try {
        observeConn.observe(state, (operation, partition, offset, row) -> {
          if (operation.equals("Delete") || operation.equals("Update") || operation.equals(
              "Insert")) {
            records.add(new Record(operation, row));
            state.setOffset(partition, offset);
          }
        });
      } catch (Exception e) {
        observeException[0] = e;
      }
    });
    t.start();

    try (Statement stmt = conn.getConnection().createStatement()) {
      for (int i = 0; i < 10; i++) {
        stmt.execute(String.format("INSERT INTO observe VALUES(%d, 3)", i));
      }

      stmt.execute("UPDATE observe SET b = 4");

      for (int i = 0; i < 10; i++) {
        stmt.execute(String.format("DELETE FROM observe WHERE a = %d", i));
      }
    }

    Thread.sleep(1000);
    ((com.singlestore.jdbc.Connection) observeConn.getConnection()).cancelCurrentQuery();
    Thread.sleep(1000);
    t.interrupt();

    assertTrue(observeException[0].getMessage().contains("Query execution was interrupted"));

    records.sort((r1, r2) -> {
      if (!r1.operation.equals(r2.operation)) {
        return r1.operation.compareTo(r2.operation);
      } else {
        return Integer.compare(r1.row.get("a").getInt(), r2.row.get("a").getInt());
      }
    });

    for (int i = 0; i < 10; i++) {
      assertEquals("Delete", records.get(i).operation);
      assertEquals((Integer) i, records.get(i).row.get("a").getInt());
      assertNull(records.get(i).row.get("b"));
    }

    for (int i = 10; i < 20; i++) {
      assertEquals("Insert", records.get(i).operation);
      assertEquals((Integer) i - 10, records.get(i).row.get("a").getInt());
      assertEquals((Integer) 3, records.get(i).row.get("b").getInt());
    }

    for (int i = 20; i < 30; i++) {
      assertEquals("Update", records.get(i).operation);
      assertEquals((Integer) i - 20, records.get(i).row.get("a").getInt());
      assertEquals((Integer) 4, records.get(i).row.get("b").getInt());
    }
  }

  @Test
  public void observeAllTypes() throws Exception {
    SingleStoreConfiguration conf = getConfig("observeAllTypes");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS observeAllTypes");
      stmt.execute(
          "CREATE ROWSTORE TABLE IF NOT EXISTS observeAllTypes (\n"
              + "    `boolColumn` BOOL DEFAULT true,\n"
              + "    `booleanColumn` BOOLEAN DEFAULT true,\n"
              + "    `bitColumn` BIT(64) DEFAULT '01234567',\n"
              + "    `tinyintColumn` TINYINT DEFAULT 124,\n"
              + "    `tinyint4Column` TINYINT(4) DEFAULT 124,\n"
              + "    `mediumintColumn` MEDIUMINT DEFAULT 8388607,\n"
              + "    `smallintColumn` SMALLINT DEFAULT 32767,\n"
              + "    `intColumn` INT DEFAULT 2147483647,\n"
              + "    `integerColumn` INTEGER DEFAULT 2147483647,\n"
              + "    `bigintColumn` BIGINT DEFAULT 9223372036854775807,\n"
              + "    `floatColumn` FLOAT DEFAULT 10.1,\n"
              + "    `doubleColumn` DOUBLE DEFAULT 100.1,\n"
              + "    `realColumn` REAL DEFAULT 100.1,\n"
              + "    `dateColumn` DATE DEFAULT '2000-10-10',\n"
              + "    `timeColumn` TIME DEFAULT '22:59:59',\n"
              + "    `time6Column` TIME(6) DEFAULT '22:59:59.111111',\n"
              + "    `datetimeColumn` DATETIME DEFAULT '2023-12-31 23:59:59',\n"
              + "    `datetime6Column` DATETIME(6) DEFAULT '2023-12-31 22:59:59.111111',\n"
              + "    `timestampColumn` TIMESTAMP DEFAULT '2022-01-19 03:14:07',\n"
              + "    `timestamp6Column` TIMESTAMP(6) DEFAULT '2022-01-19 03:14:07.111111',\n"
              + "    `yearColumn` YEAR DEFAULT '1989',\n"
              + "    `decimalColumn` DECIMAL(65, 30) DEFAULT 10000.100001,\n"
              + "    `decColumn` DEC DEFAULT 10000,\n"
              + "    `fixedColumn` FIXED DEFAULT 10000,\n"
              + "    `numericColumn` NUMERIC DEFAULT 10000,\n"
              + "    `charColumn` CHAR DEFAULT 'a',\n"
              + "    `mediumtextColumn` MEDIUMTEXT DEFAULT 'abc',\n"
              + "    `binaryColumn` BINARY DEFAULT 'a',\n"
              + "    `varcharColumn` VARCHAR(100) DEFAULT 'abc',\n"
              + "    `varbinaryColumn` VARBINARY(100) DEFAULT 'abc',\n"
              + "    `longtextColumn` LONGTEXT DEFAULT 'abc',\n"
              + "    `textColumn` TEXT DEFAULT 'abc',\n"
              + "    `tinytextColumn` TINYTEXT DEFAULT 'abc',\n"
              + "    `longblobColumn` LONGBLOB DEFAULT 'abc',\n"
              + "    `mediumblobColumn` MEDIUMBLOB DEFAULT 'abc',\n"
              + "    `blobColumn` BLOB DEFAULT 'abc',\n"
              + "    `tinyblobColumn` TINYBLOB DEFAULT 'abc',\n"
              + "    `jsonColumn` JSON DEFAULT '{}',\n"
              + "    `enum_f` ENUM('val1','val2','val3') default 'val1',\n"
              + "    `set_f` SET('v1','v2','v3') default 'v1',\n"
              + "    `geographyColumn` GEOGRAPHY DEFAULT 'POLYGON((1 1,2 1,2 2, 1 2, 1 1))',\n"
              + "    `geographypointColumn` GEOGRAPHYPOINT DEFAULT 'POINT(1.50000003 1.50000000)',\n"
              + "    `vectorColumn` VECTOR(2, I32) DEFAULT '[1, 2]',\n"
              + "    `bsonColumn` BSON,\n"
              + "     unique key(intColumn),\n"
              + "     shard key(intColumn)\n"
              + " );");
    }

    final Exception[] observeException = new Exception[1];
    SingleStoreConnection observeConn = new SingleStoreConnection(conf);
    List<Record> records = new ArrayList<>();
    Thread t = new Thread(() -> {
      try {
        observeConn.observe(new State(8), (operation, partition, offset, row) -> {
          if (operation.equals("Delete") || operation.equals("Update") || operation.equals(
              "Insert")) {
            records.add(new Record(operation, row));
          }
        });
      } catch (Exception e) {
        observeException[0] = e;
      }
    });
    t.start();

    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("INSERT INTO `observeAllTypes` VALUES (\n" +
          "TRUE, " + // boolColumn
          "TRUE, " + // booleanColumn
          "'abcdefgh', " + // bitColumn
          "-128, " + // tinyintColumn
          "-128, " + // tinyint4Column
          "-8388608, " + // mediumintColumn
          "-32768, " + // smallintColumn
          "-2147483648, " + // intColumn
          "-2147483648, " + // integerColumn
          "-9223372036854775808, " + // bigintColumn
          "-100.01, " + // floatColumn
          "-1000.01, " + // doubleColumn
          "-1000.01, " + // realColumn
          "'1000-01-01', " + // dateColumn
          "'0:00:00', " + // timeColumn
          "'0:00:00.000000', " + // time6Column
          "'1000-01-01 00:00:00', " + // datetimeColumn
          "'1000-01-01 00:00:00.000000', " + // datetime6Column
          "'1970-01-01 00:00:01', " + // timestampColumn
          "'1970-01-01 00:00:01.000000', " + // timestamp6Column
          "1901, " + // yearColumn
          "12345678901234567890123456789012345.123456789012345678901234567891, " +
          // decimalColumn
          "1234567890, " + // decColumn
          "1234567890, " + // fixedColumn
          "1234567890, " + // numericColumn
          "'a', " + // charColumn
          "'abc', " + // mediumtextColumn
          "'a', " + // binaryColumn
          "'abc', " + // varcharColumn
          "'abc', " + // varbinaryColumn
          "'abc', " + // longtextColumn
          "'abc', " + // textColumn
          "'abc', " + // tinytextColumn
          "'abc', " + // longblobColumn
          "'abc', " + // mediumblobColumn
          "'abc', " + // blobColumn
          "'abc', " + // tinyblobColumn
          "'{}', " + // jsonColumn
          "'val1', " + // enum_f
          "'v1', " + // set_f
          "'POLYGON((1 1,2 1,2 2, 1 2, 1 1))', " + // geographyColumn
          "'POINT(1.50000003 1.50000000)', " + // geographypointColumn
          "'[1, 2]', " + // vectorColumn
          "'{}')" // bsonColumn
      );

      stmt.execute("INSERT INTO `observeAllTypes` VALUES (\n" +
          "FALSE, " + // boolColumn
          "FALSE, " + // booleanColumn
          "'abcdefgh', " + // bitColumn
          "127, " + // tinyintColumn
          "127, " + // tinyint4Column
          "8388607, " + // mediumintColumn
          "32767, " + // smallintColumn
          "2147483647, " + // intColumn
          "2147483647, " + // integerColumn
          "9223372036854775807, " + // bigintColumn
          "100.01, " + // floatColumn
          "1000.01, " + // doubleColumn
          "1000.01, " + // realColumn
          "'2020-01-01', " + // dateColumn
          "'12:00:00', " + // timeColumn
          "'12:00:00.123456', " + // time6Column
          "'2020-01-01 00:00:00', " + // datetimeColumn
          "'2020-01-01 00:00:00.123456', " + // datetime6Column
          "'2020-01-01 00:00:00', " + // timestampColumn
          "'2020-01-01 00:00:00.123456', " + // timestamp6Column
          "2020, " + // yearColumn
          "12345678901234567890123456789012345.123456789012345678901234567891, " +
          // decimalColumn
          "1234567890, " + // decColumn
          "1234567890, " + // fixedColumn
          "1234567890, " + // numericColumn
          "'a', " + // charColumn
          "'abc', " + // mediumtextColumn
          "'a', " + // binaryColumn
          "'abc', " + // varcharColumn
          "'abc', " + // varbinaryColumn
          "'abc', " + // longtextColumn
          "'abc', " + // textColumn
          "'abc', " + // tinytextColumn
          "'abc', " + // longblobColumn
          "'abc', " + // mediumblobColumn
          "'abc', " + // blobColumn
          "'abc', " + // tinyblobColumn
          "'{}', " + // jsonColumn
          "'val1', " + // enum_f
          "'v1', " + // set_f
          "'POLYGON((1 1,2 1,2 2, 1 2, 1 1))', " + // geographyColumn
          "'POINT(1.50000003 1.50000000)', " + // geographypointColumn
          "'[1, 2]', " + // vectorColumn
          "'{}')" // bsonColumn
      );
    }

    Thread.sleep(1000);
    ((com.singlestore.jdbc.Connection) observeConn.getConnection()).cancelCurrentQuery();
    Thread.sleep(1000);
    t.interrupt();

    assertTrue(observeException[0].getMessage().contains("Query execution was interrupted"));

    assertEquals(2, records.size());
    records.sort(Comparator.comparingInt(r -> r.row.get("intColumn").getInt()));

    Record record = records.get(0);
    Map<String, ValueType> row = record.row;

    assertEquals((short) 1, row.get("boolColumn").getShort());
    assertEquals((short) 1, row.get("booleanColumn").getShort());
    assertEquals(ByteString.copyFrom("abcdefgh".getBytes()),
        row.get("bitColumn").getBinary());
    assertEquals((short) -128, row.get("tinyintColumn").getShort());
    assertEquals((short) -128, row.get("tinyintColumn").getShort());
    assertEquals(-8388608, row.get("mediumintColumn").getInt());
    assertEquals((short) -32768, row.get("smallintColumn").getShort());
    assertEquals(-2147483648, row.get("intColumn").getInt());
    assertEquals(-2147483648, row.get("integerColumn").getInt());
    assertEquals(-9223372036854775808L, row.get("bigintColumn").getLong());
    assertEquals((float) -100.01, row.get("floatColumn").getFloat());
    assertEquals(-1000.01, row.get("doubleColumn").getDouble());
    assertEquals(-1000.01, row.get("realColumn").getDouble());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(-30610224000L)
            .setNanos(0)
            .build(),
        row.get("dateColumn").getNaiveDate());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(0L)
            .setNanos(0)
            .build(),
        row.get("timeColumn").getNaiveDate());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(0L)
            .setNanos(0)
            .build(),
        row.get("time6Column").getNaiveDate());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(-30610224000L)
            .setNanos(0)
            .build(),
        row.get("datetimeColumn").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(-30610224000L)
            .setNanos(0)
            .build(),
        row.get("datetime6Column").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(1L)
            .setNanos(0)
            .build(),
        row.get("timestampColumn").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(1L)
            .setNanos(0)
            .build(),
        row.get("timestamp6Column").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
        .setSeconds(-2177452800L)
        .setNanos(0)
        .build(), row.get("yearColumn").getNaiveDate());
    assertEquals(
        "12345678901234567890123456789012345.123456789012345678901234567891",
        row.get("decimalColumn").getDecimal());
    assertEquals("1234567890", row.get("decColumn").getDecimal());
    assertEquals("1234567890", row.get("fixedColumn").getDecimal());
    assertEquals("1234567890", row.get("numericColumn").getDecimal());
    assertEquals("a", row.get("charColumn").getString());
    assertEquals("abc", row.get("mediumtextColumn").getString());
    assertEquals(ByteString.copyFrom("a".getBytes()), row.get("binaryColumn").getBinary());
    assertEquals("abc", row.get("varcharColumn").getString());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("varbinaryColumn").getBinary());
    assertEquals("abc", row.get("longtextColumn").getString());
    assertEquals("abc", row.get("textColumn").getString());
    assertEquals("abc", row.get("tinytextColumn").getString());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("longblobColumn").getBinary());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("mediumblobColumn").getBinary());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("blobColumn").getBinary());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("tinyblobColumn").getBinary());
    assertEquals("{}", row.get("jsonColumn").getJson());
    assertEquals("val1", row.get("enum_f").getString());
    assertEquals("v1", row.get("set_f").getString());
    assertEquals(
        "POLYGON((1.00000000 1.00000000, 2.00000000 1.00000000, 2.00000000 2.00000000, 1.00000000 2.00000000, 1.00000000 1.00000000))",
        row.get("geographyColumn").getString());
    assertEquals(
        "POINT(1.50000003 1.50000000)", row.get("geographypointColumn").getString());
    assertEquals(ByteString.copyFrom(new byte[]{1, 0, 0, 0, 2, 0, 0, 0}),
        row.get("vectorColumn").getBinary());
    assertEquals(ByteString.copyFrom(new byte[]{5, 0, 0, 0, 0}),
        row.get("bsonColumn").getBinary());

    record = records.get(1);
    row = record.row;

    assertEquals((short) 0, row.get("boolColumn").getShort());
    assertEquals((short) 0, row.get("booleanColumn").getShort());
    assertEquals(ByteString.copyFrom("abcdefgh".getBytes()),
        row.get("bitColumn").getBinary());
    assertEquals((short) 127, row.get("tinyintColumn").getShort());
    assertEquals((short) 127, row.get("tinyintColumn").getShort());
    assertEquals(8388607, row.get("mediumintColumn").getInt());
    assertEquals((short) 32767, row.get("smallintColumn").getShort());
    assertEquals(2147483647, row.get("intColumn").getInt());
    assertEquals(2147483647, row.get("integerColumn").getInt());
    assertEquals(9223372036854775807L, row.get("bigintColumn").getLong());
    assertEquals((float) 100.01, row.get("floatColumn").getFloat());
    assertEquals(1000.01, row.get("doubleColumn").getDouble());
    assertEquals(1000.01, row.get("realColumn").getDouble());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(1577836800L)
            .setNanos(0)
            .build(),
        row.get("dateColumn").getNaiveDate());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(0L)
            .setNanos(0)
            .build(),
        row.get("timeColumn").getNaiveDate());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(0L)
            .setNanos(0)
            .build(),
        row.get("time6Column").getNaiveDate());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(1577836800L)
            .setNanos(0)
            .build(),
        row.get("datetimeColumn").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(1577836800L)
            .setNanos(123456000)
            .build(),
        row.get("datetime6Column").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(1577836800L)
            .setNanos(0)
            .build(),
        row.get("timestampColumn").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
            .setSeconds(1577836800L)
            .setNanos(123456000)
            .build(),
        row.get("timestamp6Column").getNaiveDatetime());
    assertEquals(Timestamp.newBuilder()
        .setSeconds(1577836800L)
        .setNanos(0)
        .build(), row.get("yearColumn").getNaiveDate());
    assertEquals(
        "12345678901234567890123456789012345.123456789012345678901234567891",
        row.get("decimalColumn").getDecimal());
    assertEquals("1234567890", row.get("decColumn").getDecimal());
    assertEquals("1234567890", row.get("fixedColumn").getDecimal());
    assertEquals("1234567890", row.get("numericColumn").getDecimal());
    assertEquals("a", row.get("charColumn").getString());
    assertEquals("abc", row.get("mediumtextColumn").getString());
    assertEquals(ByteString.copyFrom("a".getBytes()), row.get("binaryColumn").getBinary());
    assertEquals("abc", row.get("varcharColumn").getString());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("varbinaryColumn").getBinary());
    assertEquals("abc", row.get("longtextColumn").getString());
    assertEquals("abc", row.get("textColumn").getString());
    assertEquals("abc", row.get("tinytextColumn").getString());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("longblobColumn").getBinary());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("mediumblobColumn").getBinary());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("blobColumn").getBinary());
    assertEquals(ByteString.copyFrom("abc".getBytes()), row.get("tinyblobColumn").getBinary());
    assertEquals("{}", row.get("jsonColumn").getJson());
    assertEquals("val1", row.get("enum_f").getString());
    assertEquals("v1", row.get("set_f").getString());
    assertEquals(
        "POLYGON((1.00000000 1.00000000, 2.00000000 1.00000000, 2.00000000 2.00000000, 1.00000000 2.00000000, 1.00000000 1.00000000))",
        row.get("geographyColumn").getString());
    assertEquals(
        "POINT(1.50000003 1.50000000)", row.get("geographypointColumn").getString());
    assertEquals(ByteString.copyFrom(new byte[]{1, 0, 0, 0, 2, 0, 0, 0}),
        row.get("vectorColumn").getBinary());
    assertEquals(ByteString.copyFrom(new byte[]{5, 0, 0, 0, 0}),
        row.get("bsonColumn").getBinary());
  }

  @Test
  public void observeVectorJson() throws Exception {
    SingleStoreConfiguration conf = getConfig("observeVectorJson");
    SingleStoreConnection conn = new SingleStoreConnection(conf);

    try (Statement stmt = conn.getConnection().createStatement()) {
      stmt.execute("SET GLOBAL vector_type_project_format = 'JSON'");
      stmt.execute("DROP TABLE IF EXISTS observeVectorJson");
      stmt.execute("CREATE TABLE observeVectorJson(a VECTOR(2, I32))");
      stmt.execute("INSERT INTO observeVectorJson VALUES ('[1, 2]')");
    }

    SchemaList schemaList = conn.getSchema();
    List<Schema> schemas = schemaList.getSchemasList();
    assertEquals(1, schemas.size());

    Schema schema = schemas.get(0);
    assertEquals(database, schema.getName());

    List<Table> tables = schema.getTablesList();
    assertEquals(1, tables.size());

    Table table = tables.get(0);
    assertEquals("observeVectorJson", table.getName());

    List<Column> columns = table.getColumnsList();
    assertEquals(1, columns.size());

    Column column = columns.get(0);
    assertEquals("a", column.getName());
    assertEquals(DataType.JSON, column.getType());

    final Exception[] observeException = new Exception[1];
    SingleStoreConnection observeConn = new SingleStoreConnection(conf);
    List<Record> records = new ArrayList<>();

    Thread t = new Thread(() -> {
      try {
        observeConn.observe(new State(8), (operation, partition, offset, row) -> {
          if (operation.equals("Delete") || operation.equals("Update") || operation.equals(
              "Insert")) {
            records.add(new Record(operation, row));
          }
        });
      } catch (Exception e) {
        observeException[0] = e;
      }
    });
    t.start();
    Thread.sleep(1000);
    ((com.singlestore.jdbc.Connection) observeConn.getConnection()).cancelCurrentQuery();
    Thread.sleep(1000);
    t.interrupt();

    assertTrue(observeException[0].getMessage().contains("Query execution was interrupted"));

    assertEquals(1, records.size());
    // TODO: at the moment, OBSERVE returns wrong values when `vector_type_project_format` is JSON
    // assertEquals("[1,2]", records.get(0).row.get("a").getJson());
  }
}
