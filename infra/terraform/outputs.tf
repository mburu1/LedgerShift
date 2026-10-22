output "catalog_name" {
  description = "Managed Unity Catalog catalog."
  value = databricks_catalog.lakehouse.name
}
output "schema_names" {
  description = "Schemas created for medallion and operations layers."
  value = [databricks_schema.bronze.name, databricks_schema.silver.name, databricks_schema.gold.name, databricks_schema.ops.name]
}
