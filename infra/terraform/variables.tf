variable "databricks_host" {
  description = "Workspace URL; supply through TF_VAR_databricks_host."
  type = string
  validation {
    condition = can(regex("^https://", var.databricks_host))
    error_message = "databricks_host must be an https URL."
  }
}
variable "databricks_token" {
  description = "Optional local PAT; prefer workload identity in CI."
  type = string
  sensitive = true
  default = ""
}
variable "catalog_name" {
  description = "Environment-specific Unity Catalog name."
  type = string
  default = "ledgershift_dev"
  validation {
    condition = can(regex("^[A-Za-z][A-Za-z0-9_]*$", var.catalog_name))
    error_message = "catalog_name must be a valid Unity Catalog identifier."
  }
}
variable "data_engineers_group" { type = string; default = "ledgershift_data_engineers" }
variable "analysts_group" { type = string; default = "ledgershift_analysts" }
variable "ops_group" { type = string; default = "ledgershift_ops" }
variable "owner" { type = string; default = "ledgershift" }
