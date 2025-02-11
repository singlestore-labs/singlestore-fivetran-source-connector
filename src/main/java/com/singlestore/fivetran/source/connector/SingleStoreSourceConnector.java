package com.singlestore.fivetran.source.connector;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreSourceConnector {

  private static final Logger logger = LoggerFactory.getLogger(SingleStoreSourceConnector.class);

  public static void main(String[] args) throws InterruptedException, IOException, ParseException {
    Options options = new Options();
    Option portOption = new Option("p", "port", true, "port which server will listen");
    options.addOption(portOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error("Failed to parse arguments", e);
      formatter.printHelp("singlestore-fivetran-source-connector", options);

      throw e;
    }

    String portStr = cmd.getOptionValue("port", "50051");
    int port;
    try {
      port = Integer.parseInt(portStr);
    } catch (NumberFormatException e) {
      logger.warn("Failed to parse --port option", e);
      formatter.printHelp("singlestore-fivetran-source-connector", options);

      throw e;
    }

    logger.info(
        String.format("Starting Source Connector gRPC server (version %s) which listens port %d",
            VersionProvider.getVersion(), port));
    Server server = ServerBuilder.forPort(port)
        .addService(new SingleStoreSourceConnectorServiceImpl()).build();

    server.start();
    logger.info("Source Connector gRPC server started");
    server.awaitTermination();
  }
}
