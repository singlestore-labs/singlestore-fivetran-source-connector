package com.singlestore.fivetran.source.connector;

public class StaleOffsetException extends Exception {

  public StaleOffsetException(Exception cause) {
    super("The offset the connector is trying to resume from is considered stale.\n"
            + "Therefore, the connector cannot resume streaming.\n"
            + "The only way to recover is to re-sync all historical data. For more details, refer to: https://fivetran.com/docs/connectors/troubleshooting/re-sync-a-connector\n"
            + "To help prevent failures related to stale offsets in future, you can increase the value of the following engine variables in SingleStore:\n"
            + " * 'snapshots_to_keep' - Defines the number of snapshots to keep for backup and replication;\n"
            + " * 'snapshot_trigger_size' - Defines the size of transaction logs in bytes, which, when reached, triggers a snapshot that is written to disk.\n"
            + "You may also consider decreasing the sync frequency. For more information, visit: https://fivetran.com/docs/core-concepts/syncoverview#syncfrequencyandscheduling",
        cause);
  }
}
