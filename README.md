# SingleStore Fivetran Source Connector

## Pre-requisites for Development

- JDK v17
- Gradle 8 ([Installation instructions](https://gradle.org/install/#manually))

## Steps for Starting the Server

1. Download proto files.

   ```
   wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/common.proto
   wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/connector_sdk.proto
   ```

2. Build the Jar (replace version with yours).

   ```
   ./gradlew jar -Pversion=<version>
   ```

3. Run the Jar (replace version with yours).

   ```
   java -jar build/libs/singlestore-fivetran-source-connector-<version>.jar
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
   wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/common.proto
   wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/connector_sdk.proto
   ```

6. Run tests (replace version with yours).

   ```
   ./gradlew build -Pversion=<version>
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

5. Start the Source Connector server. (replace version with yours)

   ```
   wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/common.proto
   wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/main/connector_sdk.proto
   ./gradlew jar -Pversion=<version>
   java -jar build/libs/singlestore-fivetran-source-connector-<version>.jar
   ```

6. Update the `./tester/configuration.json` file with your credentials.

7. Run the tester using
   [these](https://github.com/fivetran/fivetran_sdk/blob/v2/tools/source-connector-tester/README.md)
   instructions.
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

## Release process

To release a new version:

1. Push a version tag using semantic versioning with a `v` prefix (`v<major>.<minor>.<patch>`, for example `v1.2.3`):

   ```bash
   git tag v1.0.1
   git push origin v1.0.1
   ```

   The package version is derived from the tag (the leading `v` is stripped). This triggers the [CI workflow](.github/workflows/ci.yml), which:

   - Runs the test matrix
   - Builds the connector JAR with the release version
   - Creates a [GitHub Release](https://github.com/singlestore-labs/singlestore-fivetran-source-connector/releases) with auto-generated release notes and the JAR (`singlestore-fivetran-source-connector-<version>.jar`)

2. After the GitHub Release is published, post a message in the `#ext-fivetran-singlestore` Slack channel asking Fivetran to upload the updated connector. Replace `<version>` with the release version (for example, `1.2.9` for tag `v1.2.9`):

   ```
   Hi,

   We have released SingleStore Fivetran Source Connector <version>.
   GitHub Release: https://github.com/singlestore-labs/singlestore-fivetran-source-connector/releases/tag/v<version>

   Could you please upload the updated version?

   Thanks,
   <Your name>
   ```