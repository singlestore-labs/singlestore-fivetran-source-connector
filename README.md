# SingleStore Fivetran Source Connector

## Pre-requisites for Development

- JDK v17
- Gradle 8 ([Installation instructions](https://gradle.org/install/#manually))

## Steps for Starting the Server

1. Download proto files.
   
   ```
   wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/common.proto
   wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/connector_sdk.proto
   ```

2. Build the Jar.

   ```
   gradle jar
   ```

3. Run the Jar.

   ```
   java -jar build/libs/singlestore-fivetran-source-connector-0.0.4.jar
   ```

## Steps for Running Java Tests

1. Start the SingleStore deployment.
   You must specify a valid SingleStore license in `SINGLESTORE_LICENSE` and a password in
   `ROOT_PASSWORD`.

   ```
   docker run \
       -d --name singlestoredb-dev \
       -e SINGLESTORE_LICENSE=<YOUR SINGLESTORE LICENSE> \
       -e ROOT_PASSWORD=<YOUR SINGLESTORE ROOT PASSWORD> \
       -e SINGLESTORE_VERSION="8.7.16" \
       -p 3306:3306 -p 8080:8080 -p 9000:9000 \
       ghcr.io/singlestore-labs/singlestoredb-dev:latest
   ```

2. Wait for the database to start.

3. Enable `OBSERVE` queries.
   
   ```
   SET GLOBAL enable_observe_queries = 1;
   ```

4. Create `ROOT_PASSWORD` environment variable.

   ```
   export ROOT_PASSWORD="<YOUR SINGLESTORE ROOT PASSWORD>"
   ```

5. Download proto files.
   
   ```
   wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/common.proto
   wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/connector_sdk.proto
   ```

6. Run tests.

   ```
   gradle build
   ```

## Steps for Using Source Connector Tester

1. Start the SingleStore deployment.
   You must specify a valid SingleStore license in `SINGLESTORE_LICENSE` and a password in
   `ROOT_PASSWORD`.

   ```
   docker run \
       -d --name singlestoredb-dev \
       -e SINGLESTORE_LICENSE=<YOUR SINGLESTORE LICENSE> \
       -e ROOT_PASSWORD=<YOUR SINGLESTORE ROOT PASSWORD> \
       -e SINGLESTORE_VERSION="8.7.16" \
       -p 3306:3306 -p 8080:8080 -p 9000:9000 \
       ghcr.io/singlestore-labs/singlestoredb-dev:latest
   ```

2. Wait for database to start.

3. Enable `OBSERVE` queries .

   ```
   SET GLOBAL enable_observe_queries = 1;
   ```

4. Create a database and table.

   ```
   DROP DATABASE IF EXISTS tester;
   CREATE DATABASE tester;
   USE tester;
   CREATE TABLE t(a INT PRIMARY KEY, b INT);
   ```

5. Start the Source Connector server.

   ```
   wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/common.proto
   wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/connector_sdk.proto
   gradle jar
   java -jar build/libs/singlestore-fivetran-source-connector-0.0.4.jar
   ```

6. Update the `./tester/configuration.json` file with your credentials.

7. Run the tester using
   [these](https://github.com/fivetran/fivetran_sdk/blob/v2/tools/source-connector-tester/README.md) instructions.
   Use the following command:

   ```
   docker run --mount type=bind,source=<PATH TO PROJECT>/tester,target=/data -a STDIN -a STDOUT -a STDERR -it -e GRPC_HOSTNAME=localhost --network=host us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<tag> --tester-type source --port 50051
   ```

8. Update the table.

   ```
   INSERT INTO t VALUES(1, 2);
   INSERT INTO t VALUES(2, 2);
   DELETE FROM t WHERE a = 1;
   UPDATE t SET b = 3 WHERE a = 2;
   ```

9. Check the content of `./tester/warehouse.db` file.
   using [DuckDB](https://duckdb.org/docs/api/cli/overview.html) CLI
   or [DBeaver](https://duckdb.org/docs/guides/sql_editors/dbeaver)
