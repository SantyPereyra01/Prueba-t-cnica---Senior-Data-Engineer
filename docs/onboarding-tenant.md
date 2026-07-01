# Onboarding de un tenant

## Prerrequisitos

- Código corto en minúsculas y owner de negocio identificado.
- Acceso confirmado a la fuente y clasificación de datos aprobada.
- Rango de backfill y SLA acordados.
- Schemas, external locations y grants creados para el ambiente objetivo.

## Procedimiento

1. Crear `config/tenants/<code>.yaml` con `tenant.code`, nombre y overrides estrictamente necesarios.
2. Agregar el código a `tenants.enabled` en `config/base.yaml`.
3. En cloud, aplicar el módulo Terraform para schemas `bronze_<code>`, `silver_<code>` y `gold_<code>`, external locations y grants de mínimo privilegio.
4. Confirmar que el origen emite el código esperado en `pais`. No agregar un `if` específico al código del pipeline.
5. Ejecutar un smoke test de un día en `dev`:

   ```bash
   uv run saas-pipeline run --env dev --tenant xx --start-date 2025-06-01 --end-date 2025-06-01
   ```

6. Revisar los conteos de Bronze/Silver/Gold, cuarentena y todos los registros de `quality_logs` para el `run_id`.
7. Ejecutar el backfill completo con `quality.fail_on_critical: true` en QA.
8. Documentar owner, SLA, fuente y grants; promover la configuración a main mediante pull request.

## Criterios de aceptación

- La reejecución del mismo rango no cambia conteos ni duplica hechos.
- No existe más de una versión corriente por material.
- Gold reconcilia unidades e ingresos contra Silver.
- Un fallo del nuevo tenant no detiene tenants existentes cuando `fail_fast` está desactivado.
- Cuarentena y quality logs incluyen tenant, batch y run identificables.

## Rollback

Deshabilitar el tenant en `tenants.enabled` detiene nuevas corridas masivas sin borrar datos. Las tablas Delta permiten restaurar una versión anterior; la retención y el procedimiento de `RESTORE` deben definirse operativamente antes de producción.
