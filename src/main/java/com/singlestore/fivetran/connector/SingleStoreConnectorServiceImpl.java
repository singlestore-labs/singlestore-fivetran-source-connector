package com.singlestore.fivetran.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.singlestore.fivetran.connector.JDBCUtil.ThrowingConsumer;
import fivetran_sdk.Checkpoint;
import fivetran_sdk.Column;
import fivetran_sdk.ConfigurationFormRequest;
import fivetran_sdk.ConfigurationFormResponse;
import fivetran_sdk.ConfigurationTest;
import fivetran_sdk.ConnectorGrpc;
import fivetran_sdk.DropdownField;
import fivetran_sdk.FormField;
import fivetran_sdk.LogEntry;
import fivetran_sdk.LogLevel;
import fivetran_sdk.OpType;
import fivetran_sdk.Operation;
import fivetran_sdk.Record;
import fivetran_sdk.SchemaList;
import fivetran_sdk.SchemaRequest;
import fivetran_sdk.SchemaResponse;
import fivetran_sdk.TestRequest;
import fivetran_sdk.TestResponse;
import fivetran_sdk.TextField;
import fivetran_sdk.UpdateRequest;
import fivetran_sdk.UpdateResponse;
import io.grpc.stub.StreamObserver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SingleStoreConnectorServiceImpl extends ConnectorGrpc.ConnectorImplBase {

  private static final Logger logger =
      LoggerFactory.getLogger(SingleStoreConnectorServiceImpl.class);

  @Override
  public void configurationForm(ConfigurationFormRequest request,
      StreamObserver<ConfigurationFormResponse> responseObserver) {
    responseObserver.onNext(ConfigurationFormResponse.newBuilder()
        .setSchemaSelectionSupported(true).setTableSelectionSupported(true)
        .addAllFields(Arrays.asList(
            FormField.newBuilder().setName("host").setLabel("Host").setRequired(true)
                .setTextField(TextField.PlainText).build(),
            FormField.newBuilder().setName("port").setLabel("Port").setRequired(true)
                .setTextField(TextField.PlainText).build(),
            FormField.newBuilder().setName("database").setLabel("Database")
                .setRequired(true)
                .setTextField(TextField.PlainText).build(),
            FormField.newBuilder().setName("table").setLabel("Table")
                .setRequired(true)
                .setTextField(TextField.PlainText).build(),
            FormField.newBuilder().setName("user").setLabel("Username")
                .setRequired(true).setTextField(TextField.PlainText).build(),
            FormField.newBuilder().setName("password").setLabel("Password")
                .setRequired(false).setTextField(TextField.Password).build(),
            FormField.newBuilder().setName("ssl.mode").setLabel("SSL mode")
                .setRequired(false)
                .setDescription(
                    "Whether to use an encrypted connection to SingleStore.\n"
                        + "Options include:\n"
                        + " * 'disable' to use an unencrypted connection (the default);\n"
                        + " * 'trust' to use a secure (encrypted) connection (no certificate and hostname validation);\n"
                        + " * 'verify_ca' to use a secure (encrypted) connection but additionally verify the server TLS certificate against the configured Certificate Authority "
                        + "(CA) certificates, or fail if no valid matching CA certificates are found;\n"
                        + " * 'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.")
                .setDropdownField(DropdownField.newBuilder()
                    .addDropdownField("disable").addDropdownField("trust")
                    .addDropdownField("verify_ca")
                    .addDropdownField("verify-full"))
                .build(),
            FormField.newBuilder().setName("ssl.server.cert")
                .setLabel("SSL Server's Certificate").setRequired(false)
                .setDescription(
                    "Server's certificate in DER format or the server's CA certificate. "
                        + "The certificate is added to the trust store, which allows the connection to trust a self-signed certificate.")
                .setTextField(TextField.PlainText).build(),
            FormField.newBuilder().setName("driver.parameters")
                .setLabel("Driver Parameters").setRequired(false)
                .setDescription(
                    "Additional JDBC parameters to use with connection string to SingleStore server.\n"
                        + "Format: 'param1=value1; param2 = value2; ...'.\n"
                        + "The supported parameters are available in the https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters .")
                .setTextField(TextField.PlainText).build()))
        .addAllTests(Arrays.asList(
            ConfigurationTest.newBuilder().setName("connect").setLabel("Tests connection").build(),
            ConfigurationTest.newBuilder().setName("table").setLabel("Tests table existence")
                .build()
        ))
        .build());

    responseObserver.onCompleted();
  }

  @Override
  public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
    String testName = request.getName();
    SingleStoreConfiguration configuration =
        new SingleStoreConfiguration(request.getConfigurationMap());

    try {
      if (testName.equals("connect")) {
        JDBCUtil.checkConnection(configuration);
      } else if (testName.equals("table")) {
        JDBCUtil.checkTableExistence(configuration);
      }
    } catch (Exception e) {
      logger.warn("Test failed", e);

      responseObserver.onNext(TestResponse.newBuilder().setSuccess(false)
          .setFailure(e.getMessage()).build());
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onNext(TestResponse.newBuilder().setSuccess(true).build());
    responseObserver.onCompleted();
  }

  @Override
  public void schema(SchemaRequest request, StreamObserver<SchemaResponse> responseObserver) {
    SingleStoreConfiguration configuration = new SingleStoreConfiguration(
        request.getConfigurationMap());

    try {
      SchemaList schema = JDBCUtil.getSchema(configuration);
      responseObserver.onNext(SchemaResponse.newBuilder().setWithSchema(schema).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn(String.format("SchemaRequest failed for %s",
          JDBCUtil.escapeTable(configuration.database(), configuration.table())), e);

      responseObserver.onNext(
          SchemaResponse.newBuilder().setSchemaResponseNotSupported(true).build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
    SingleStoreConfiguration configuration = new SingleStoreConfiguration(
        request.getConfigurationMap());

    try {
      State state;
      if (request.hasStateJson()) {
        state = State.fromJson(request.getStateJson());
      } else {
        state = new State(JDBCUtil.getNumPartitions(configuration));
      }

      responseObserver.onNext(UpdateResponse.newBuilder()
          .setLogEntry(LogEntry.newBuilder()
              .setLevel(LogLevel.INFO)
              .setMessage("Sync STARTING")
              .build())
          .build());

      JDBCUtil.observe(configuration, state, new ThrowingConsumer<ResultSet>() {

        private String bytesToHex(byte[] bytes) {
          char[] res = new char[bytes.length * 2];

          int j = 0;
          for (int i = 0; i < bytes.length; i++) {
            res[j++] = Character.forDigit((bytes[i] >> 4) & 0xF, 16);
            res[j++] = Character.forDigit((bytes[i] & 0xF), 16);
          }

          return new String(res);
        }

        final List<Column> rowColumns = JDBCUtil.getRowColumns(configuration);
        final List<Column> keyColumns = JDBCUtil.getKeyColumns(configuration);

        @Override
        public void accept(ResultSet rs) throws SQLException, JsonProcessingException {
          switch (rs.getString("Type")) {
            case "Insert":
              responseObserver.onNext(
                  UpdateResponse.newBuilder()
                      .setOperation(
                          Operation.newBuilder()
                              .setRecord(
                                  Record.newBuilder()
                                      .setSchemaName(configuration.database())
                                      .setTableName(configuration.table())
                                      .setType(OpType.UPSERT)
                                      .putAllData(JDBCUtil.getRow(rs, rowColumns))
                                      .build()
                              ).build()
                      ).build()
              );
              break;
            case "Update":
              responseObserver.onNext(
                  UpdateResponse.newBuilder()
                      .setOperation(
                          Operation.newBuilder()
                              .setRecord(
                                  Record.newBuilder()
                                      .setSchemaName(configuration.database())
                                      .setTableName(configuration.table())
                                      .setType(OpType.UPDATE)
                                      .putAllData(JDBCUtil.getRow(rs, rowColumns))
                                      .build()
                              ).build()
                      ).build()
              );
              break;
            case "Delete":
              responseObserver.onNext(
                  UpdateResponse.newBuilder()
                      .setOperation(
                          Operation.newBuilder()
                              .setRecord(
                                  Record.newBuilder()
                                      .setSchemaName(configuration.database())
                                      .setTableName(configuration.table())
                                      .setType(OpType.DELETE)
                                      .putAllData(JDBCUtil.getRow(rs, keyColumns))
                                      .build()
                              ).build()
                      ).build()
              );
              break;
            default:
              return;
          }

          Integer partition = rs.getInt("PartitionId");
          String offset = bytesToHex(rs.getBytes("Offset"));
          state.setOffset(partition, offset);
          responseObserver.onNext(
              UpdateResponse.newBuilder()
                  .setOperation(
                      Operation.newBuilder()
                          .setCheckpoint(
                              Checkpoint.newBuilder()
                                  .setStateJson(state.toJson())
                                  .build()).build()
                  ).build()
          );
        }
      });

      responseObserver.onNext(UpdateResponse.newBuilder()
          .setLogEntry(LogEntry.newBuilder()
              .setLevel(LogLevel.INFO)
              .setMessage("Sync DONE")
              .build())
          .build());
    } catch (Exception e) {
      responseObserver.onError(e);
    }

    responseObserver.onCompleted();
  }
}
