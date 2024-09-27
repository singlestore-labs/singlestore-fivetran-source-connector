package com.singlestore.fivetran.connector;

import com.google.protobuf.ByteString;
import fivetran_sdk.Column;
import fivetran_sdk.DataType;
import fivetran_sdk.DecimalParams;
import fivetran_sdk.Schema;
import fivetran_sdk.SchemaList;
import fivetran_sdk.Table;
import fivetran_sdk.ValueType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JDBCUtil {

  static Connection createConnection(SingleStoreConfiguration conf) throws Exception {
    Properties connectionProps = new Properties();
    connectionProps.put("user", conf.user());
    if (conf.password() != null) {
      connectionProps.put("password", conf.password());
    }
    connectionProps.put("transformedBitIsBoolean", "true");
    connectionProps.put("connectionAttributes",
        String.format("_connector_name:%s,_connector_version:%s",
            "SingleStore Fivetran Connector", VersionProvider.getVersion()));

    connectionProps.put("sslMode", conf.sslMode());
    if (!conf.sslMode().equals("disable")) {
      putIfNotEmpty(connectionProps, "serverSslCert", conf.sslServerCert());
    }
    String driverParameters = conf.driverParameters();
    if (driverParameters != null) {
      for (String parameter : driverParameters.split(";")) {
        String[] keyValue = parameter.split("=");
        if (keyValue.length != 2) {
          throw new Exception("Invalid value of `driverParameters` configuration");
        }
        putIfNotEmpty(connectionProps, keyValue[0], keyValue[1]);
      }
    }

    String url = String.format("jdbc:singlestore://%s:%d/%s", conf.host(), conf.port(),
        conf.database());

    try {
      return DriverManager.getConnection(url, connectionProps);
    } catch (SQLException e) {
      if (e.getErrorCode() == 1046 && e.getSQLState().equals("3D000")
          && conf.database() != null) {
        url = String.format("jdbc:singlestore://%s:%d/%s", conf.host(), conf.port(),
            conf.database());
        return DriverManager.getConnection(url, connectionProps);
      }

      throw e;
    }
  }

  private static void putIfNotEmpty(Properties props, String key, String value) {
    if (key != null && !key.trim().isEmpty() && value != null && !value.trim().isEmpty()) {
      props.put(key.trim(), value.trim());
    }
  }

  public static void checkConnection(SingleStoreConfiguration conf) throws Exception {
    try (Connection conn = createConnection(conf);
        Statement stmt = conn.createStatement();) {
      stmt.execute("SELECT 1");
    }
  }

  public static void checkTableExistence(SingleStoreConfiguration conf) throws Exception {
    try (Connection conn = createConnection(conf);
        Statement stmt = conn.createStatement();) {
      stmt.executeQuery(
          String.format("SELECT * FROM %s WHERE 1=0", escapeTable(conf.database(), conf.table())));
    }
  }

  public static String escapeTable(String database, String table) {
    return escapeIdentifier(database) + "." + escapeIdentifier(table);
  }

  public static String escapeIdentifier(String ident) {
    return String.format("`%s`", ident.replace("`", "``"));
  }

  public static String escapeString(String literal) {
    return String.format("'%s'", literal.replace("'", "''"));
  }

  public static SchemaList getSchema(SingleStoreConfiguration conf) throws Exception {
    try (Connection conn = createConnection(conf)) {
      DatabaseMetaData metadata = conn.getMetaData();

      Set<String> primaryKeyColumns = new HashSet<>();
      try (ResultSet primaryKeysRS = metadata.getPrimaryKeys(conf.database(), null, conf.table())) {
        while (primaryKeysRS.next()) {
          primaryKeyColumns.add(primaryKeysRS.getString("COLUMN_NAME"));
        }
      }

      List<Column> columns = new ArrayList<>();
      try (ResultSet columnsRS = metadata.getColumns(conf.database(), null, conf.table(), null)) {
        while (columnsRS.next()) {
          Column.Builder c = Column.newBuilder()
              .setName(columnsRS.getString("COLUMN_NAME"))
              .setType(mapDataTypes(columnsRS.getString("TYPE_NAME")))
              .setPrimaryKey(
                  primaryKeyColumns.contains(columnsRS.getString("COLUMN_NAME")));
          if (c.getType() == DataType.DECIMAL) {
            c.setDecimal(DecimalParams.newBuilder()
                .setScale(columnsRS.getInt("DECIMAL_DIGITS"))
                .setPrecision(columnsRS.getInt("COLUMN_SIZE")).build());
          }
          columns.add(c.build());
        }
      }

      return SchemaList.newBuilder()
          .addSchemas(
              Schema.newBuilder()
                  .setName(conf.database())
                  .setTables(0,
                      Table.newBuilder()
                          .setName(conf.table())
                          .addAllColumns(columns)
                  )
          ).build();
    }
  }

  static DataType mapDataTypes(String typeName) {
    switch (typeName) {
      case "BOOLEAN":
        return DataType.BOOLEAN;
      case "SMALLINT":
        return DataType.SHORT;
      case "MEDIUMINT":
      case "INT":
        return DataType.INT;
      case "BIGINT":
        return DataType.LONG;
      case "FLOAT":
        return DataType.FLOAT;
      case "DOUBLE":
        return DataType.DOUBLE;
      case "DECIMAL":
        return DataType.DECIMAL;
      case "DATE":
      case "YEAR":
        return DataType.NAIVE_DATE;
      case "DATETIME":
      case "TIME":
      case "TIMESTAMP":
        return DataType.NAIVE_DATETIME;
      case "BIT":
      case "BINARY":
      case "VARBINARY":
      case "TINYBLOB":
      case "MEDIUMBLOB":
      case "BLOB":
      case "LONGBLOB":
      case "BSON":
        return DataType.BINARY;
      case "CHAR":
      case "VARCHAR":
      case "TINYTEXT":
      case "MEDIUMTEXT":
      case "TEXT":
      case "LONGTEXT":
      case "GEOGRAPHYPOINT":
      case "GEOGRAPHY":
      case "VECTOR":
        return DataType.STRING;
      case "JSON":
        return DataType.JSON;
      default:
        return DataType.UNSPECIFIED;
    }
  }

  static Integer getNumPartitions(SingleStoreConfiguration conf) throws Exception {
    try (Connection conn = createConnection(conf);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            String.format(
                "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = %s",
                escapeString(conf.database())
            ))
    ) {
      if (!rs.next()) {
        throw new Exception("Failed to retrieve number of partitions in the database");
      }
      return rs.getInt("num_partitions");
    }
  }

  static void observe(SingleStoreConfiguration conf, State state,
      ThrowingConsumer<ResultSet> handler)
      throws Exception {
    try (Connection conn = createConnection(conf);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            String.format(
                "OBSERVE * FROM %s BEGIN AT (%s)",
                escapeTable(conf.database(), conf.table()),
                state.offsetsAsSQL()
            ))) {
      while (rs.next()) {
        handler.accept(rs);
      }
    }
  }

  @FunctionalInterface
  public interface ThrowingConsumer<T> {

    void accept(T t) throws Exception;
  }

  static Map<String, ValueType> getRow(ResultSet rs, List<Column> columns) throws SQLException {
    Map<String, ValueType> res = new HashMap<>();
    for (Column column : columns) {
      res.put(column.getName(), getValue(rs, column));
    }

    return res;
  }

  static ValueType getValue(ResultSet rs, Column column) throws SQLException {
    switch (column.getType()) {
      case BOOLEAN:
        return ValueType.newBuilder()
            .setBool(rs.getBoolean(column.getName()))
            .build();
      case SHORT:
        return ValueType.newBuilder()
            .setShort(rs.getShort(column.getName()))
            .build();
      case INT:
        return ValueType.newBuilder()
            .setInt(rs.getInt(column.getName()))
            .build();
      case LONG:
        return ValueType.newBuilder()
            .setLong(rs.getLong(column.getName()))
            .build();
      case FLOAT:
        return ValueType.newBuilder()
            .setFloat(rs.getFloat(column.getName()))
            .build();
      case DOUBLE:
        return ValueType.newBuilder()
            .setDouble(rs.getDouble(column.getName()))
            .build();
      case DECIMAL:
        return ValueType.newBuilder()
            .setDecimal(rs.getString(column.getName()))
            .build();
      case NAIVE_DATE:
        return ValueType.newBuilder()
            .setNaiveDate(convertTimestamps(rs.getTimestamp(column.getName())))
            .build();
      case NAIVE_DATETIME:
        return ValueType.newBuilder()
            .setNaiveDatetime(convertTimestamps(rs.getTimestamp(column.getName())))
            .build();
      case BINARY:
        return ValueType.newBuilder()
            .setBinary(ByteString.copyFrom(rs.getBytes(column.getName())))
            .build();
      case JSON:
        return ValueType.newBuilder()
            .setJson(rs.getString(column.getName()))
            .build();
      case STRING:
      default:
        return ValueType.newBuilder()
            .setString(rs.getString(column.getName()))
            .build();
    }
  }

  private static com.google.protobuf.Timestamp convertTimestamps(Timestamp t) {
    long seconds = t.getTime() / 1000;
    int nanos = t.getNanos();

    return com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(seconds)
        .setNanos(nanos)
        .build();
  }

  static List<Column> getRowColumns(SingleStoreConfiguration conf) throws Exception {
    List<Column> res = new ArrayList<>();
    boolean hasKey = false;

    SchemaList schemaList = getSchema(conf);
    for (Schema schema : schemaList.getSchemasList()) {
      for (Table table : schema.getTablesList()) {
        for (Column column : table.getColumnsList()) {
          res.add(column);
          if (column.getPrimaryKey()) {
            hasKey = true;
          }
        }
      }
    }

    if (!hasKey) {
      res.add(Column.newBuilder()
          .setName("InternalId")
          .setType(DataType.LONG)
          .setPrimaryKey(true)
          .build()
      );
    }

    return res;
  }

  static List<Column> getKeyColumns(SingleStoreConfiguration conf) throws Exception {
    List<Column> res = new ArrayList<>();
    boolean hasKey = false;

    SchemaList schemaList = getSchema(conf);
    for (Schema schema : schemaList.getSchemasList()) {
      for (Table table : schema.getTablesList()) {
        for (Column column : table.getColumnsList()) {
          if (column.getPrimaryKey()) {
            res.add(column);
            hasKey = true;
          }
        }
      }
    }

    if (!hasKey) {
      res.add(Column.newBuilder()
          .setName("InternalId")
          .setType(DataType.LONG)
          .setPrimaryKey(true)
          .build()
      );
    }

    return res;
  }
}
