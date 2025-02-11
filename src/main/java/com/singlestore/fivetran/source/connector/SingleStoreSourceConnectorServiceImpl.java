package com.singlestore.fivetran.source.connector;

import fivetran_sdk.v2.Checkpoint;
import fivetran_sdk.v2.ConditionalFields;
import fivetran_sdk.v2.ConfigurationFormRequest;
import fivetran_sdk.v2.ConfigurationFormResponse;
import fivetran_sdk.v2.ConfigurationTest;
import fivetran_sdk.v2.RecordType;
import fivetran_sdk.v2.SourceConnectorGrpc;
import fivetran_sdk.v2.DropdownField;
import fivetran_sdk.v2.FormField;
import fivetran_sdk.v2.Record;
import fivetran_sdk.v2.SchemaList;
import fivetran_sdk.v2.SchemaRequest;
import fivetran_sdk.v2.SchemaResponse;
import fivetran_sdk.v2.SchemaSelection;
import fivetran_sdk.v2.Selection;
import fivetran_sdk.v2.TableSelection;
import fivetran_sdk.v2.TablesWithSchema;
import fivetran_sdk.v2.TestRequest;
import fivetran_sdk.v2.TestResponse;
import fivetran_sdk.v2.TextField;
import fivetran_sdk.v2.UpdateRequest;
import fivetran_sdk.v2.UpdateResponse;
import fivetran_sdk.v2.VisibilityCondition;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreSourceConnectorServiceImpl extends
    SourceConnectorGrpc.SourceConnectorImplBase {

  private static final Logger logger = LoggerFactory.getLogger(
      SingleStoreSourceConnectorServiceImpl.class);

  @Override
  public void configurationForm(ConfigurationFormRequest request,
      StreamObserver<ConfigurationFormResponse> responseObserver) {
    FormField serverCert = FormField.newBuilder().setName("ssl.server.cert")
        .setLabel("SSL Server's Certificate").setRequired(false)
        .setDescription(
            "Server's certificate in DER format or the server's CA certificate. "
                + "The certificate is added to the trust store, which allows the connection to trust a self-signed certificate.")
        .setTextField(TextField.PlainText)
        .build();

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
                        + " * 'verify-ca' to use a secure (encrypted) connection but additionally verify the server TLS certificate against the configured Certificate Authority "
                        + "(CA) certificates, or fail if no valid matching CA certificates are found;\n"
                        + " * 'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.")
                .setDropdownField(DropdownField.newBuilder()
                    .addDropdownField("disable")
                    .addDropdownField("trust")
                    .addDropdownField("verify-ca")
                    .addDropdownField("verify-full"))
                .build(),
            FormField.newBuilder()
                .setConditionalFields(
                    ConditionalFields.newBuilder()
                        .setCondition(VisibilityCondition.newBuilder()
                            .setConditionField("ssl.mode")
                            .setStringValue("trust")
                            .build()
                        )
                        .addAllFields(
                            Collections.singletonList(serverCert))
                        .build()
                ).build(),
            FormField.newBuilder()
                .setConditionalFields(
                    ConditionalFields.newBuilder()
                        .setCondition(VisibilityCondition.newBuilder()
                            .setConditionField("ssl.mode")
                            .setStringValue("verify-ca")
                            .build()
                        )
                        .addAllFields(
                            Collections.singletonList(serverCert))
                        .build()
                ).build(),
            FormField.newBuilder()
                .setConditionalFields(
                    ConditionalFields.newBuilder()
                        .setCondition(VisibilityCondition.newBuilder()
                            .setConditionField("ssl.mode")
                            .setStringValue("verify-full")
                            .build()
                        )
                        .addAllFields(
                            Collections.singletonList(serverCert))
                        .build()
                ).build(),
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
                .build()))
        .build());

    responseObserver.onCompleted();
  }

  @Override
  public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
    String testName = request.getName();
    SingleStoreConfiguration configuration = new SingleStoreConfiguration(
        request.getConfigurationMap());
    SingleStoreConnection conn = new SingleStoreConnection(configuration);

    try {
      if (testName.equals("connect")) {
        conn.checkConnection();
      } else if (testName.equals("table")) {
        conn.checkTableExistence();
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
  public void schema(SchemaRequest request, StreamObserver<SchemaResponse>
      responseObserver) {
    SingleStoreConfiguration configuration = new SingleStoreConfiguration(
        request.getConfigurationMap());
    SingleStoreConnection conn = new SingleStoreConnection(configuration);

    try {
      SchemaList schema = conn.getSchema();
      responseObserver.onNext(SchemaResponse.newBuilder().setWithSchema(schema).
          build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn(String.format("SchemaRequest failed for %s",
          SingleStoreConnection.escapeTable(configuration.database(), configuration.table())), e);

      responseObserver.onNext(
          SchemaResponse.newBuilder().setSchemaResponseNotSupported(true).build());
      responseObserver.onCompleted();
    }
  }

  /**
   * Returns a set of column names that should be selected. If no selection is provided - returns
   * null. In this case all columns should be selected. If configured database and table are not
   * selected - returns an empty list. In this case, no columns are populated.
   *
   * @param request an absolute URL giving the base location of the image
   * @param conf    the location of the image, relative to the url argument
   * @return set of column names to select, null if select all columns
   */
  private Set<String> getSelectedColumns(UpdateRequest request, SingleStoreConfiguration conf) {
    if (!request.hasSelection()) {
      return null;
    }
    Selection sel = request.getSelection();

    if (!sel.hasWithSchema()) {
      return null;
    }
    TablesWithSchema tablesWithSchema = sel.getWithSchema();

    for (SchemaSelection schemaSelection : tablesWithSchema.getSchemasList()) {
      if (!schemaSelection.getIncluded() || !schemaSelection.getSchemaName()
          .equals(conf.database())) {
        continue;
      }

      for (TableSelection tableSelection : schemaSelection.getTablesList()) {
        if (!tableSelection.getIncluded() || !tableSelection.getTableName().equals(conf.table())) {
          continue;
        }

        Map<String, Boolean> columns = tableSelection.getColumnsMap();
        Set<String> selectedColumns = new HashSet<>();
        for (String columnName : columns.keySet()) {
          if (columns.get(columnName)) {
            selectedColumns.add(columnName);
          }
        }

        return selectedColumns;
      }
    }

    return new HashSet<>();
  }

  int CHECKPOINT_BATCH_SIZE = 10_000;

  @Override
  public void update(UpdateRequest request, StreamObserver<UpdateResponse>
      responseObserver) {
    SingleStoreConfiguration configuration = new SingleStoreConfiguration(
        request.getConfigurationMap());
    SingleStoreConnection conn = new SingleStoreConnection(configuration);
    Set<String> selectedColumns = getSelectedColumns(request, configuration);
    AtomicLong recordsRead = new AtomicLong();

    try {
      State state;
      if (request.hasStateJson() && !request.getStateJson().equals("{}")) {
        state = State.fromJson(request.getStateJson());
      } else {
        state = new State(conn.getNumPartitions());
      }

      logger.info("Sync STARTING");

      conn.observe(state, selectedColumns, (operation, partition, offset, row) -> {
        switch (operation) {
          case "Insert":
            responseObserver.onNext(
                UpdateResponse.newBuilder()
                    .setRecord(
                        Record.newBuilder()
                            .setSchemaName(configuration.database())
                            .setTableName(configuration.table())
                            .setType(RecordType.UPSERT)
                            .putAllData(row)
                            .build())
                    .build());
            break;
          case "Update":
            responseObserver.onNext(
                UpdateResponse.newBuilder()
                    .setRecord(
                        Record.newBuilder()
                            .setSchemaName(configuration.database())
                            .setTableName(configuration.table())
                            .setType(RecordType.UPDATE)
                            .putAllData(row)
                            .build())
                    .build());
            break;
          case "Delete":
            responseObserver.onNext(
                UpdateResponse.newBuilder()
                    .setRecord(
                        Record.newBuilder()
                            .setSchemaName(configuration.database())
                            .setTableName(configuration.table())
                            .setType(RecordType.DELETE)
                            .putAllData(row)
                            .build())
                    .build());
            break;
          default:
            return;
        }

        state.setOffset(partition, offset);
        if (recordsRead.incrementAndGet() % CHECKPOINT_BATCH_SIZE == 0) {
          responseObserver.onNext(
              UpdateResponse.newBuilder()
                  .setCheckpoint(
                      Checkpoint.newBuilder()
                          .setStateJson(state.toJson())
                          .build())
                  .build());
        }
      });

      if (recordsRead.incrementAndGet() % CHECKPOINT_BATCH_SIZE != 0) {
        responseObserver.onNext(
            UpdateResponse.newBuilder()
                .setCheckpoint(
                    Checkpoint.newBuilder()
                        .setStateJson(state.toJson())
                        .build())
                .build());
      }

      logger.info("Sync DONE");
    } catch (Exception e) {
      if (e.getMessage().contains("The requested Offset is too stale")) {
        responseObserver.onError(new StaleOffsetException(e));
      } else {
        responseObserver.onError(e);
      }
    }

    responseObserver.onCompleted();
  }
}
