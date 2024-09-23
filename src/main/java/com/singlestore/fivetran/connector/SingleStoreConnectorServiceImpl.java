package com.singlestore.fivetran.connector;

import fivetran_sdk.*;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.Collections;

public class SingleStoreConnectorServiceImpl extends ConnectorGrpc.ConnectorImplBase {
  @Override
  public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
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
        .addAllTests(Collections.singletonList(ConfigurationTest.newBuilder()
            .setName("connect").setLabel("Tests connection").build()))
        .build());

    responseObserver.onCompleted();
  }

  @Override
  public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
    // TODO: implement
  }

  @Override
  public void schema(SchemaRequest request, StreamObserver<SchemaResponse> responseObserver) {
    // TODO: implement
  }

  @Override
  public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
    // TODO: implement
  }
}
