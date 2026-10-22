resource "databricks_group" "data_engineers" { display_name = var.data_engineers_group }
resource "databricks_group" "analysts" { display_name = var.analysts_group }
resource "databricks_group" "ops" { display_name = var.ops_group }

resource "databricks_catalog" "lakehouse" {
  name = var.catalog_name
  comment = "LedgerShift managed lakehouse catalog"
  properties = {
    project = "LedgerShift"
    environment = var.catalog_name == "ledgershift_prod" ? "prod" : "dev"
    managed_by = "terraform"
  }
}
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.lakehouse.name
  name = "bronze"
  comment = "Append-only source ingestion"
}
resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.lakehouse.name
  name = "silver"
  comment = "Validated, deduplicated and conformed data"
}
resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.lakehouse.name
  name = "gold"
  comment = "Curated analytical models"
}
resource "databricks_schema" "ops" {
  catalog_name = databricks_catalog.lakehouse.name
  name = "ops"
  comment = "Operational metadata and reconciliation evidence"
}
