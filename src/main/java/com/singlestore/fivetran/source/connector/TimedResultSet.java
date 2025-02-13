package com.singlestore.fivetran.source.connector;

import java.sql.ResultSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimedResultSet implements AutoCloseable {

  private final ResultSet resultSet;
  ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

  private TimedResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public static TimedResultSet from(ResultSet resultSet) {
    return new TimedResultSet(resultSet);
  }

  @Override
  public void close() {
    try {
      executor.shutdownNow();

      if (!resultSet.isClosed()) {
        ((com.singlestore.jdbc.Connection) resultSet.getStatement()
            .getConnection()).cancelCurrentQuery();
      }
    } catch (Exception ignored) {
    }
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public Boolean next() throws InterruptedException, ExecutionException {
    Future<Boolean> future = executor.submit(resultSet::next);
    try {
      // Get the result with a timeout of 1 second
      return future.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      return false;
    }
  }
}
