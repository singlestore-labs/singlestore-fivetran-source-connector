---
name: Setup Guide
title: SingleStore source connector by fivetran | Setup Guide
description: Read step-by-step instructions on how to connect SingleStore with your destination using Fivetran connectors.
---

# SingleStore Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="singlestore_source" /%}

Follow our setup guide to connect SingleStore to Fivetran.

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related to SingleStore connector and its documentation, contact SingleStore by raising an issue in the [SingleStore Fivetran Connector](https://github.com/singlestore-labs/singlestore-fivetran-connector) GitHub repository.

-----

## Prerequisites

To connect your SingleStore database to Fivetran, you need:

- A SingleStore instance of version 8.7.16 or higher. Refer to SingleStore's [Creating and Using Workspaces documentation](https://docs.singlestore.com/cloud/getting-started-with-singlestore-helios/about-workspaces/creating-and-using-workspaces/) for instructions on creating a SingleStore workspace in the [Cloud Portal](https://portal.singlestore.com/). To deploy a Self-Managed cluster instead, refer to SingleStore's [Deploy documentation](https://docs.singlestore.com/db/latest/deploy/). Once the SingleStore workspace/cluster is Active, you'll need the following connection configuration parameters to connect to Fivetran: 
    - `Host`
    - `Port`
    - `Username`
    - `Password`
    - `Database`
    - `Table`
- A Fivetran account with a role having the [Create Connection](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#rbacpermissions) permission.

---

## Setup instructions

To authorize Fivetran to connect to your SinlgeStore database, follow these instructions:

### <span class="step-item">Configure SingleStore</span>

1. Configure your firewall and/or other access control systems to allow incoming connections to your SingleStore instance from [Fivetran's IPs](https://fivetran.com/docs/using-fivetran/ips) for your region.
2. Ensure that the SingleStore database user has the `SELECT` permission.
3. Enable support of OBSERVE queries:

   ```
   SET GLOBAL enable_observe_queries=1
   ```

4. (Optional) Configure `snapshots_to_keep` and `snapshot_trigger_size` [engine variables](https://docs.singlestore.com/cloud/reference/configuration-reference/engine-variables/list-of-engine-variables/). At some point, `offsets` will be considered stale, meaning the connection will no longer be able to retrieve data associated with that logical point in the WAL (Write-Ahead Log). In practical terms, `offsets` become stale once they are older than the oldest snapshot in the system. The `snapshots_to_keep` and `snapshot_trigger_size` variables control the number and size of snapshots providing you with some control over the data retention window. If you increase `snapshots_to_keep` or `snapshot_trigger_size`, offsets will become stale later. If offsets become stale, the connection will be unable to continue streaming change events, and the only way to resolve this is to re-sync all data.

   ```
   SET GLOBAL snapshot_trigger_size=10737418240
   SET GLOBAL snapshots_to_keep=4
   ```

### <span class="step-item">Finish Fivetran configuration </span>

1. Log in to your Fivetran account.
2. Go to the [**Connections** page](https://fivetran.com/dashboard/connectors), and then click **+ Add connection**.
3. Select **SingleStore** as the connector type.
4. In the connection setup form, enter the following connection configuration parameters for you SingleStore workspace/cluster:
    - **Host**
    - **Port**
    - **Database**
    - **Table**
    - **Username**
    - **Password**
5. (Optional) Enable SSL and specify related configuration parameters.
6. (Optional) Specify additional **Driver Parameters**. Refer to [The SingleStore JDBC Driver](https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver/#connection-string-parameters) documentation for a list of supported parameters.
7. (Not applicable to Hybrid Deployment) Copy the [Fivetran's IP addresses (or CIDR)](/docs/using-fivetran/ips) that you _must_ safelist in your firewall.
8. Click **Save & Test**.

### Setup tests

Fivetran performs the following SingleStore connection tests:

- The Connection test checks if Fivetran can connect to your SingleStore cluster using credentials provided in the setup form
- The Table test checks if specified table exists

### <span class="step-item">(Optional) Post-setup changes</span>

We recommend to adjust the sync frequency in Fivetran so that syncs run more often. This will reduce the likelihood of `offsets` becoming stale and help avoid the need for a full re-sync.

1. Go to the **Setup** tab of the connection details page.
2. Change **Sync frequency** to **15 minutes**.

---

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Connector Overview](/docs/connectors/databases/singlestore)

<b> </b>

[<i aria-hidden="true" class="material-icons">account_tree</i> Schema Information](/docs/connectors/databases/singlestore#schemainformation)

<b> </b>

[<i aria-hidden="true" class="material-icons">settings</i> API Connection Configuration](/docs/rest-api/api-reference/connections/create-connection?service=singlestore_source)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
