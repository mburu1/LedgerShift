resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.lakehouse.name
  grant {
    principal  = databricks_group.data_engineers.display_name
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }
  grant {
    principal  = databricks_group.ops.display_name
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "bronze" {
  schema = databricks_schema.bronze.id
  grant {
    principal  = databricks_group.data_engineers.display_name
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
  grant {
    principal  = databricks_group.ops.display_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "silver" {
  schema = databricks_schema.silver.id
  grant {
    principal  = databricks_group.data_engineers.display_name
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
  grant {
    principal  = databricks_group.ops.display_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "gold" {
  schema = databricks_schema.gold.id
  grant {
    principal  = databricks_group.analysts.display_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = databricks_group.data_engineers.display_name
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
  grant {
    principal  = databricks_group.ops.display_name
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "ops" {
  schema = databricks_schema.ops.id
  grant {
    principal  = databricks_group.data_engineers.display_name
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
  grant {
    principal  = databricks_group.ops.display_name
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
  }
}
