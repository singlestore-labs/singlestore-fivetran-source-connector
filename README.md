# SingleStore Fivetran Connector

## Pre-requisites for development

- JDK v17
- Gradle 8 ([here](https://gradle.org/install/#manually) is an installation instruction)

## Steps for starting server

1. Download proto files

```
wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/common.proto
wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/connector_sdk.proto
```

2. Build the Jar

```
gradle jar
```

3. Run the Jar

```
java -jar build/libs/singlestore-fivetran-connector-0.0.3.jar
```

## Steps for running Java tests

1. Start SingleStore cluster
   You must insert a valid SingleStore license as SINGLESTORE_LICENSE and a password as
   ROOT_PASSWORD

```
docker run \
    -d --name singlestoredb-dev \
    -e SINGLESTORE_LICENSE=<YOUR SINGLESTORE LICENSE> \
    -e ROOT_PASSWORD=<YOUR SINGLESTORE ROOT PASSWORD> \
    -e SINGLESTORE_VERSION="8.7.16" \
    -p 3306:3306 -p 8080:8080 -p 9000:9000 \
    ghcr.io/singlestore-labs/singlestoredb-dev:latest
```

2. Wait for database to start

3. Enable OBSERVE queries support

```
SET GLOBAL enable_observe_queries = 1;
```

4. Create `ROOT_PASSWORD` environment variable

```
export ROOT_PASSWORD="YOUR SINGLESTORE ROOT PASSWORD"
```

5. Download proto files

```
wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/common.proto
wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/connector_sdk.proto
```

6. Run tests

```
gradle build
```

## Steps for using Connector tester

1. Start SingleStore cluster
   You must insert a valid SingleStore license as SINGLESTORE_LICENSE and a password as
   ROOT_PASSWORD

```
docker run \
    -d --name singlestoredb-dev \
    -e SINGLESTORE_LICENSE=<YOUR SINGLESTORE LICENSE> \
    -e ROOT_PASSWORD=<YOUR SINGLESTORE ROOT PASSWORD> \
    -e SINGLESTORE_VERSION="8.7.16" \
    -p 3306:3306 -p 8080:8080 -p 9000:9000 \
    ghcr.io/singlestore-labs/singlestoredb-dev:latest
```

2. Wait for database to start

3. Enable OBSERVE queries support

```
SET GLOBAL enable_observe_queries = 1;
```

4. Create database and table

```
DROP DATABASE IF EXISTS tester;
CREATE DATABASE tester;
USE tester;
CREATE TABLE t(a INT PRIMARY KEY, b INT);
```

5. Start Connector server

```
wget -O src/main/proto/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/common.proto
wget -O src/main/proto/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/connector_sdk.proto
gradle jar
java -jar build/libs/singlestore-fivetran-connector-0.0.3.jar
```

6. Update the `./tester/configuration.json` file with your credentials

7. Run the tester by following instructions
   from [here](https://github.com/fivetran/fivetran_sdk/blob/main/tools/destination-tester/README.md).
   As a command use you can use

```
docker run --mount type=bind,source=<PATH TO PROJECT>/tester,target=/data -a STDIN -a STDOUT -a STDERR -it -e GRPC_HOSTNAME=localhost --network=host fivetrandocker/fivetran-sdk-tester:0.24.0729.001 --tester-type source --port 55051
```

8. Update table

```
INSERT INTO t VALUES(1, 2);
INSERT INTO t VALUES(2, 2);
DELETE FROM t WHERE a = 1;
UPDATE t SET b = 3 WHERE a = 2;
```

9. Check the content of `./tester/warehouse.db` file
   using [DuckDB](https://duckdb.org/docs/api/cli/overview.html) CLI
   or [DBeaver](https://duckdb.org/docs/guides/sql_editors/dbeaver)
