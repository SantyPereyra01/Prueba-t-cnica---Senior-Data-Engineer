# Infraestructura para onboarding

Terraform provisionaría el contenedor/rutas de ADLS, credenciales o access connector, external locations, schemas de Unity Catalog, grants y secretos de acceso a la fuente. El job y su identidad deberían recibir solo `USE CATALOG`, `USE SCHEMA`, lectura de RAW y escritura en los schemas del tenant.

El estado remoto, providers y naming corporativo quedarían en el root module. El siguiente módulo ilustrativo muestra los recursos específicos del tenant; los nombres de metastore, storage credential y principals se inyectan, no se hardcodean.

```hcl
variable "catalog_name"          { type = string }
variable "tenant"               { type = string }
variable "storage_root"          { type = string }
variable "storage_credential"    { type = string }
variable "pipeline_principal"    { type = string }

locals {
  layers = toset(["bronze", "silver", "gold"])
}

resource "databricks_schema" "tenant" {
  for_each     = local.layers
  catalog_name = var.catalog_name
  name         = "${each.key}_${var.tenant}"
  comment      = "SAAS ${each.key} layer for tenant ${var.tenant}"
  properties = {
    tenant = var.tenant
    layer  = each.key
  }
}

resource "databricks_external_location" "tenant" {
  name            = "saas_${var.tenant}"
  url             = "${var.storage_root}/${var.tenant}"
  credential_name = var.storage_credential
  comment         = "Tenant-isolated SAAS storage"
}

resource "databricks_grants" "schema" {
  for_each = databricks_schema.tenant
  schema   = "${var.catalog_name}.${each.value.name}"

  grant {
    principal  = var.pipeline_principal
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
}

resource "databricks_secret_scope" "tenant" {
  name = "saas-${var.tenant}"
}

output "schemas" {
  value = { for layer, schema in databricks_schema.tenant : layer => schema.name }
}
```

En una implementación real agregaría validaciones de variables, grants diferenciados para readers/writers, CMK, private endpoints y diagnósticos de ADLS. Los secretos se cargarían desde un gestor corporativo; Terraform crearía el scope y permisos, pero no almacenaría valores sensibles en código o outputs.
