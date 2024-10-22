package com.singlestore.fivetran.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class SingleStoreConnection {

  SingleStoreConfiguration conf;
  private Connection conn;
  private VectorTypeProjectFormat vectorTypeProjectFormat;

  private enum VectorTypeProjectFormat {
    JSON,
    BINARY
  }

  public SingleStoreConnection(SingleStoreConfiguration conf) {
    this.conf = conf;
  }

  public VectorTypeProjectFormat getVectorTypeProjectFormat() throws Exception {
    if (vectorTypeProjectFormat == null) {
      try (Statement stmt = getConnection().createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT @@vector_type_project_format");
        if (!rs.next()) {
          throw new Exception("Failed to retrieve vector_type_project_format");
        }

        if (rs.getString("@@vector_type_project_format").equals("BINARY")) {
          vectorTypeProjectFormat = VectorTypeProjectFormat.BINARY;
        } else {
          vectorTypeProjectFormat = VectorTypeProjectFormat.JSON;
        }
      }
    }

    return vectorTypeProjectFormat;
  }

  Connection getConnection() throws Exception {
    if (conn == null || conn.isClosed()) {
      Properties connectionProps = new Properties();
      connectionProps.put("user", conf.user());
      if (conf.password() != null) {
        connectionProps.put("password", conf.password());
      }
      connectionProps.put("tinyInt1isBit", "false");
      connectionProps.put("defaultFetchSize", "1");
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

      conn = DriverManager.getConnection(url, connectionProps);
    }

    return conn;
  }

  private void putIfNotEmpty(Properties props, String key, String value) {
    if (key != null && !key.trim().isEmpty() && value != null && !value.trim().isEmpty()) {
      props.put(key.trim(), value.trim());
    }
  }

  public void checkConnection() throws Exception {
    try (Statement stmt = getConnection().createStatement();) {
      stmt.execute("SELECT 1");
    }
  }

  public void checkTableExistence() throws Exception {
    try (Statement stmt = getConnection().createStatement();) {
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

  public SchemaList getSchema() throws Exception {

    Connection conn = getConnection();
    DatabaseMetaData metadata = conn.getMetaData();

    Set<String> primaryKeyColumns = new HashSet<>();
    try (ResultSet primaryKeysRS = metadata.getPrimaryKeys(conf.database(), null,
        conf.table())) {
      while (primaryKeysRS.next()) {
        primaryKeyColumns.add(primaryKeysRS.getString("COLUMN_NAME"));
      }
    }

    List<Column> columns = new ArrayList<>();
    try (ResultSet columnsRS = metadata.getColumns(conf.database(), null,
        conf.table(), null)) {
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
                .addTables(Table.newBuilder()
                    .setName(conf.table())
                    .addAllColumns(columns)))
        .build();
  }

  private DataType mapDataTypes(String typeName) throws Exception {
    switch (typeName) {
      case "BOOLEAN":
        return DataType.BOOLEAN;
      case "TINYINT":
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
      case "ENUM":
      case "SET":
        return DataType.STRING;
      case "VECTOR":
        if (getVectorTypeProjectFormat() == VectorTypeProjectFormat.BINARY) {
          return DataType.BINARY;
        } else {
          return DataType.JSON;
        }
      case "JSON":
        return DataType.JSON;
      default:
        return DataType.UNSPECIFIED;
    }
  }

  public Integer getNumPartitions() throws Exception {
    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
            String.format(
                "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = %s"
                ,
                escapeString(conf.database())))) {
      if (!rs.next()) {
        throw new
            Exception("Failed to retrieve number of partitions in the database");
      }
      return rs.getInt("num_partitions");
    }
  }


  @FunctionalInterface
  public interface ObserveConsumer {

    void accept(String operation, Integer partition, String offset, Map<String, ValueType> row)
        throws JsonProcessingException;
  }

  public void observe(State state, Set<String> selectedColumns, ObserveConsumer consumer)
      throws Exception {
    List<Column> columns = getSchema()
        .getSchemas(0)
        .getTables(0)
        .getColumnsList();
    if (selectedColumns != null) {
      columns = columns.stream()
          .filter(column -> selectedColumns.contains(column.getName()))
          .collect(Collectors.toList());
    }

    List<Column> pkColumns = columns
        .stream()
        .filter(Column::getPrimaryKey)
        .collect(Collectors.toList());

    try (Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
            String.format(
                "OBSERVE * FROM %s BEGIN AT (%s)",
                escapeTable(conf.database(), conf.table()),
                state.offsetsAsSQL()))) {
      while (rs.next()) {
        String operation = rs.getString("Type");
        Integer partition = rs.getInt("PartitionId");
        String offset = bytesToHex(rs.getBytes("Offset"));

        if (operation.equals("Delete")) {
          consumer.accept(operation, partition, offset, getRow(rs, pkColumns));
        } else {
          consumer.accept(operation, partition, offset, getRow(rs, columns));
        }
      }
    }
  }

  private String bytesToHex(byte[] bytes) {
    char[] res = new char[bytes.length * 2];

    int j = 0;
    for (int i = 0; i < bytes.length; i++) {
      res[j++] = Character.forDigit((bytes[i] >> 4) & 0xF, 16);
      res[j++] = Character.forDigit((bytes[i] & 0xF), 16);
    }

    return new String(res);
  }

  private Map<String, ValueType> getRow(ResultSet rs, List<Column> columns)
      throws SQLException {
    Map<String, ValueType> res = new HashMap<>();
    for (Column column : columns) {
      res.put(column.getName(), getValue(rs, column));
    }

    return res;
  }

  private ValueType getValue(ResultSet rs, Column column) throws SQLException {
    if (rs.getObject(column.getName()) == null) {
      return ValueType.newBuilder()
          .setNull(true)
          .build();
    }

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

  private com.google.protobuf.Timestamp convertTimestamps(Timestamp t) {
    LocalDateTime local = t.toLocalDateTime();
    long seconds = local.toEpochSecond(ZoneOffset.UTC);
    int nanos = local.getNano();

    return com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(seconds)
        .setNanos(nanos)
        .build();
  }
}
