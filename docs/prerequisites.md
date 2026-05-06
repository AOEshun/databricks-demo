# Layer 1 Prerequisites — Azure Storage Side

This document walks a Unity Catalog admin through the one-time Azure and Databricks setup
required before deploying the Databricks Asset Bundle for this demo. SQL Server / Lakeflow
Connect prerequisites are out of scope here and are covered separately in a later slice.

---

## Overview

The parquet ingest pipeline reads from an Azure Storage container via a Unity Catalog
**External Location**. Before the bundle can be deployed and the setup notebook can
create the External Volume, the following resources must exist:

| Layer | Resource | Purpose |
|---|---|---|
| Azure | Azure Access Connector (Managed Identity) | Identity that Databricks uses to access Storage |
| Azure | Role assignment on Storage Account | Grants the Managed Identity read access |
| Databricks UC | Storage Credential | Wraps the Managed Identity inside UC |
| Databricks UC | External Location | Points at the Azure Storage container |
| Databricks UC | Privilege grants | Lets the workspace user create a catalog + volume |

---

## Step 1 — Create an Azure Access Connector (Managed Identity)

An Azure Access Connector is an Azure resource that exposes a system-assigned Managed
Identity to Databricks.

### Azure Portal

1. In the [Azure Portal](https://portal.azure.com), search for **Access Connector for Azure Databricks**.
2. Click **+ Create**.
3. Select your **Subscription** and **Resource Group** (use the same Resource Group as your
   Databricks workspace for simplicity).
4. Enter a **Name** (e.g. `adb-access-connector-demo`).
5. Leave **Identity type** as **System assigned**.
6. Click **Review + create** → **Create**.
7. After deployment, open the resource and note the **Object (principal) ID** of the
   system-assigned identity — you will need it in Step 2.

### Azure CLI equivalent

```bash
az databricks access-connector create \
  --resource-group <your-resource-group> \
  --name adb-access-connector-demo \
  --location <your-region> \
  --identity-type SystemAssigned
```

---

## Step 2 — Grant the Managed Identity access to the Storage Account

The Managed Identity needs at minimum **Storage Blob Data Reader** on the container (or the
Storage Account) that holds the parquet files. Grant **Storage Blob Data Contributor** if
the pipeline needs to write (e.g. checkpoints in the same container).

### Azure Portal

1. Navigate to your **Storage Account** in the Azure Portal.
2. Go to **Access Control (IAM)** → **+ Add role assignment**.
3. Role: **Storage Blob Data Contributor** (or Reader if write access is not needed).
4. Assign access to: **Managed identity**.
5. Click **+ Select members**, search for the Access Connector you created in Step 1
   (use the Object ID noted above), and select it.
6. Click **Review + assign** → **Assign**.

### Azure CLI equivalent

```bash
# Get the principal ID of the access connector
PRINCIPAL_ID=$(az databricks access-connector show \
  --resource-group <your-resource-group> \
  --name adb-access-connector-demo \
  --query identity.principalId -o tsv)

# Get the storage account resource ID
STORAGE_ID=$(az storage account show \
  --resource-group <your-resource-group> \
  --name <your-storage-account> \
  --query id -o tsv)

# Assign the role
az role assignment create \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

---

## Step 3 — Create a Unity Catalog Storage Credential

A **Storage Credential** is the UC object that wraps the Azure Managed Identity.

### Databricks UI

1. Open your Databricks workspace.
2. Go to **Catalog** (the catalog icon in the left sidebar) → **External data** →
   **Credentials** → **+ Add a credential**.
3. **Credential type**: Azure Managed Identity.
4. **Credential name**: `demo-storage-credential` (note this name — you will use it when
   creating the External Location).
5. **Access Connector ID**: the full Azure Resource ID of the Access Connector, e.g.
   `/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/adb-access-connector-demo`.
6. Click **Create**.

### Databricks CLI equivalent

```bash
databricks storage-credentials create \
  --json '{
    "name": "demo-storage-credential",
    "azure_managed_identity": {
      "access_connector_id": "/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/adb-access-connector-demo"
    }
  }'
```

---

## Step 4 — Create a Unity Catalog External Location

An **External Location** maps a `abfss://` path on Azure Storage to a UC-managed access
point. The setup notebook will create an External Volume that points at a sub-path of
this External Location.

### Databricks UI

1. In the Databricks workspace, go to **Catalog** → **External data** → **External
   Locations** → **+ Add an external location**.
2. **External location name**: choose a name (e.g. `demo-parquet-location`). The bundle
   doesn't reference this name directly — it uses `parquet_storage_location` (an `abfss://`
   URL pointing at a sub-folder of the External Location). The name is only used when
   granting privileges in Step 5.
3. **URL**: `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/`
   (the root of the container holding the parquet files).
4. **Storage credential**: select `demo-storage-credential` from Step 3.
5. Click **Create**.
6. Use **Test connection** to verify the Managed Identity can list blobs in the container.

### Databricks CLI equivalent

```bash
databricks external-locations create \
  --json '{
    "name": "demo-parquet-location",
    "url": "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
    "credential_name": "demo-storage-credential"
  }'
```

---

## Step 5 — Grant UC Privileges to the Workspace User

The user (or service principal) that runs `databricks bundle deploy` and the setup
notebook needs the following Unity Catalog privileges.

### Minimum required grants

| Privilege | Object | Reason |
|---|---|---|
| `CREATE CATALOG` | Metastore | To create the `DEMO` catalog |
| `CREATE EXTERNAL VOLUME` | External Location `demo-parquet-location` | To mount the volume in setup |
| `READ FILES` | External Location `demo-parquet-location` | To read parquet files via the volume |
| `WRITE FILES` | External Location `demo-parquet-location` | For Auto Loader checkpoint writes (Slice 2) |

### Grant via Databricks UI

1. Go to **Catalog** → **External data** → **External Locations** → select
   `demo-parquet-location`.
2. Click the **Permissions** tab → **Grant**.
3. Grant `CREATE EXTERNAL VOLUME`, `READ FILES`, and `WRITE FILES` to the user or
   service principal that runs the bundle.

For `CREATE CATALOG`, a metastore admin must run:

```sql
GRANT CREATE CATALOG ON METASTORE TO `<user-or-sp>`;
```

### Grant via SQL

```sql
-- Run as a metastore admin in the SQL editor or a notebook
GRANT CREATE CATALOG ON METASTORE TO `andyeshun7@gmail.com`;

GRANT CREATE EXTERNAL VOLUME, READ FILES, WRITE FILES
  ON EXTERNAL LOCATION `demo-parquet-location`
  TO `andyeshun7@gmail.com`;
```

---

## Step 6 — Verify the Setup

Before running the bundle, confirm:

- [ ] The Access Connector exists and has a system-assigned Managed Identity.
- [ ] The Managed Identity has **Storage Blob Data Contributor** on the Storage Account.
- [ ] The Storage Credential `demo-storage-credential` is visible in **Catalog → External
      data → Credentials** and shows **Active**.
- [ ] The External Location `demo-parquet-location` exists, its **Test connection** passes,
      and the parquet files (`ORDER_HEADER*.parquet`, `ORDER_DETAIL*.parquet`) are visible
      under the container root.
- [ ] The deploying user has `CREATE CATALOG` on the metastore and
      `CREATE EXTERNAL VOLUME` / `READ FILES` / `WRITE FILES` on the External Location.

Once all boxes are checked, fill in the `# TODO` placeholders in `databricks.yml` and
proceed to `databricks bundle validate -t dev`.

---

## Notes

- The External Location URL must end with a trailing `/`.
- The parquet files must reside directly under a `parquet/` sub-folder within the container
  (e.g. `abfss://container@account.dfs.core.windows.net/parquet/ORDER_HEADER*.parquet`).
  The setup notebook creates the External Volume pointing at this `parquet/` sub-path.
- If the workspace is in a VNet with private endpoints, ensure the Access Connector's
  Managed Identity can reach the Storage Account over the private endpoint.
- These steps cover the Azure Storage side only. SQL Server / Lakeflow Connect prerequisites
  are deferred to Slice 4.
