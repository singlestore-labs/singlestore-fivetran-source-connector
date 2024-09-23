package com.singlestore.fivetran.connector;

import fivetran_sdk.*;
import io.grpc.stub.StreamObserver;

public class SingleStoreConnectorServiceImpl extends ConnectorGrpc.ConnectorImplBase {
  @Override
  public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
    // TODO: implement
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
