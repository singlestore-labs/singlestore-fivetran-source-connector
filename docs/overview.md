---
name: SingleStore
title: SingleStore connector by Fivetran | Fivetran documentation
Description: Connect your SingleStore data to your destination using Fivetran.
---

# SingleStore {% badge text="Partner-Built" /%} {% availabilityBadge connector="singlestore_source" /%}

[SingleStore](https://www.singlestore.com/) is a distributed, cloud-native database that can handle transactional and analytical workloads with a unified engine. It provides real-time analytics, transactions, and streaming capabilities, enabling users to handle diverse workloads on a single platform.

> NOTE: This connector utilizes SingleStore's [OBSERVE](https://docs.singlestore.com/cloud/reference/sql-reference/data-manipulation-language-dml/observe/) queries that are currently in a preview state. As such, they are intended for experimental use only.

> WARNING: This connector currently doesn't work with [Unlimited Storage Databases](https://docs.singlestore.com/db/v8.7/manage-data/local-and-unlimited-database-storage-concepts/).

------------------

## Features

{% featureTable connector="singlestore_source" /%}

------------------

## Setup guide

Follow our [step-by-step SingleStore setup guide](/docs/connectors/databases/singlestore/setup-guide) to connect SingleStore with your destination using Fivetran connectors.

------------------

## Sync overview

Once Fivetran is connected to your SingleStore deployment, the connection fetches an initial consistent snapshot of all data from your SingleStore table. Once the initial sync is complete, the connection streams `UPDATE`/`DELETE`/`INSERT` operations made to your SingleStore table.

This connector uses [OBSERVE](https://docs.singlestore.com/cloud/reference/sql-reference/data-manipulation-language-dml/observe/) to capture change events.

This connector does not support handling schema changes. You cannot run `ALTER` and `DROP` queries while the `OBSERVE` query is running.

------------------

## Schema information

Fivetran replicates a single table of the SingleStore database. Selected SingleStore database is mapped to Fivetran schema.

### Fivetran-generated columns

Fivetran adds the following columns to table in your destination:

- `_fivetran_deleted` (BOOLEAN) marks deleted rows in the source database.
- `_fivetran_synced` (UTC TIMESTAMP) indicates when Fivetran last successfully synced the row.
- `InternalId` (BINARY) is a unique ID added to distinguish rows in tables that do not have a primary key.

### Type transformations and mapping

As we extract your data, we match SingleStore data types in your SingleStore database to types that Fivetran supports. If we don't support a specific data type, we automatically change that type to the closest supported type.

The following table illustrates how we transform your SingleStore data types into Fivetran supported types:

| SingleStore Data Type | Fivetran Data Type | Notes                                                                                                                                  |
|-----------------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| BOOL                  | SHORT              |
| BIT                   | BINARY             |
| TINYINT               | SHORT              |
| SMALLINT              | SHORT              |
| MEDIUMINT             | INT                |
| INT                   | INT                |
| BIGINT                | LONG               |
| FLOAT                 | FLOAT              |
| DOUBLE                | DOUBLE             |
| DECIMAL               | DECIMAL            |
| DATE                  | NAIVE_DATE         |
| TIME                  | NAIVE_DATETIME     |
| TIME(6)               | NAIVE_DATETIME     |
| DATETIME              | NAIVE_DATETIME     |
| DATETIME(6)           | NAIVE_DATETIME     |
| TIMESTAMP             | NAIVE_DATETIME     |
| TIMESTAMP(6)          | NAIVE_DATETIME     |
| YEAR                  | NAIVE_DATE         |
| CHAR                  | STRING             |
| VARCHAR               | STRING             |
| TINYTEXT              | STRING             |
| TEXT                  | STRING             |
| MEDIUMTEXT            | STRING             |
| LONGTEXT              | STRING             |
| BINARY                | BINARY             |
| VARBINARY             | BINARY             |
| TINYBLOB              | BINARY             |
| BLOB                  | BINARY             |
| MEDIUMBLOB            | BINARY             |
| LONGBLOB              | BINARY             |
| JSON                  | JSON               |
| BSON                  | BINARY             |
| GEOGRAPHY             | STRING             |
| GEOGRAPHYPOINT        | STRING             |
| ENUM                  | STRING             |
| SET                   | STRING             |
| VECTOR                | BINARY/JSON        | If the `vector_type_project_format` variable is set to `BINARY`, then `VECTOR` is mapped to `BINARY`, otherwise it is mapped to `JSON` |
