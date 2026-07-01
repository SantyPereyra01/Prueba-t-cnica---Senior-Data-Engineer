# Observaciones a la arquitectura

Estas observaciones no cambian el contrato de la prueba. Documentan decisiones que convendría validar mediante ADR antes de llevar el pipeline a producción.

## 1. Partición redundante por tenant en Bronze

**Observación.** La arquitectura define simultáneamente una ruta física por tenant (`bronze/<tenant>/deliveries`) y particionado por `tenant_id`. Dentro de esa ruta, `tenant_id` tiene cardinalidad uno; crear otra carpeta `_tenant_id=sv` no aporta pruning y aumenta el número de particiones pequeñas.

**Resolución aplicada.** Se implementan ambas columnas de partición (`fecha_proceso`, `_tenant_id`) para cumplir literalmente la arquitectura. Se conserva esta observación porque `_tenant_id` tiene cardinalidad uno dentro de cada ruta local por tenant y, por lo tanto, no agrega pruning efectivo.

**Alternativa.** Si Bronze fuera una única tabla cross-tenant, sí la particionaría por `_tenant_id` y fecha. Esto simplificaría operaciones globales, pero reduciría el aislamiento físico y cambiaría el mapping uno-a-uno con schemas de Unity Catalog.

## 2. La clave MERGE puede colapsar eventos legítimos

**Observación.** La clave `(tenant_id, fecha_proceso, transporte, ruta, material, tipo_entrega)` no incluye un identificador de entrega ni número de línea. Dos entregas reales del mismo material en una ruta y fecha serían indistinguibles y la última sobrescribiría a la anterior.

**Resolución aplicada.** Se usa exactamente la clave provista. En el dataset entregado, sus únicas colisiones son los 15 duplicados exactos intencionales, eliminados antes del MERGE.

**Propuesta.** Exigir en el contrato de origen `delivery_id` + `line_id`, o generar una clave estable a partir de un conjunto acordado de atributos. Un hash de toda la fila evita colisiones técnicas, pero convierte una corrección en una fila nueva y por eso no reemplaza un identificador de negocio.

## 3. Semántica incompleta para correcciones y deletes

**Observación.** `MERGE` con update/insert mantiene filas Silver que desaparecieron de una partición Bronze corregida. Por ejemplo, si origen elimina una entrega errónea y se reprocesa la fecha, Bronze queda correcto pero Silver conserva el registro anterior.

**Resolución aplicada.** Se respeta el MERGE prescrito y no se borran hechos unilateralmente.

**Propuesta.** Para fuentes snapshot, usar `WHEN NOT MATCHED BY SOURCE THEN DELETE` acotado al tenant/rango procesado. Para fuentes CDC, modelar explícitamente operaciones de delete y conservar tombstones. La elección depende del contrato de la fuente, hoy ambiguo.

## 4. Vigencia SCD2 inclusiva en ambos extremos

**Observación.** El join solicitado usa `BETWEEN`, por lo tanto ambos extremos son inclusivos. Esto exige que una versión termine el día anterior a la siguiente; si dos intervalos comparten una fecha, un hecho se duplica.

**Resolución aplicada.** Se usa el join inclusivo requerido y se validan intervalos básicos y una única versión corriente. El catálogo entregado usa intervalos no superpuestos.

**Propuesta.** Estandarizar intervalos semiabiertos `[valid_from, valid_to)`, práctica que permite que una versión termine exactamente cuando comienza la siguiente. También agregaría un check de solapamiento con self-join o ventanas antes de aceptar cambios de dimensión.

## 5. Precedencia de anomalías combinadas

**Ambigüedad.** Una fila puede tener simultáneamente tipo de entrega fuera de alcance y una cantidad inválida. La tabla de políticas no define si debe descartarse o cuarentenarse.

**Resolución aplicada.** Primero se descartan tipos fuera del alcance; las validaciones de cuarentena se aplican a candidatos analíticos. De esta forma `COBR`/`Z99` no inflan la cola de revisión manual. Cada descarte se contabiliza en `quality_logs`.

**Alternativa.** En un entorno regulado, priorizaría cuarentena sobre descarte para no ocultar problemas del origen. La precedencia debería formar parte de una matriz de reglas versionada.

## 6. Severidad y lectura de métricas de calidad

`business_key_conflicts` se clasifica como `critical` porque dos payloads distintos con la misma clave vuelven ambiguo qué hecho debería llegar a Silver y Gold. A diferencia de una cantidad negativa o un material desconocido, no existe una corrección determinística por fila; el contrato de identidad de origen está roto.

Esas filas también forman parte de `quarantined_records`, que mide el volumen total enviado a revisión. Ambas métricas son perspectivas superpuestas y no deben sumarse: una representa el total de cuarentena y la otra identifica su subconjunto crítico por conflicto de identidad.

## 7. Evolución tecnológica (horizonte 2-3)

Propondría tres mejoras después de estabilizar el batch:

1. Declarative Pipelines/Delta Live Tables o jobs declarativos para expectativas, lineage y operación gestionada.
2. Auto Loader con schema location, rescued data y checkpoints por tenant para ingesta incremental.
3. Bundles de Databricks + Terraform para desplegar jobs, grants y schemas de forma promovible entre ambientes, con monitoreo de SLA y costos.

El trade-off es mayor dependencia de la plataforma Databricks. Conviene introducirlo cuando el volumen, frecuencia y carga operativa justifiquen abandonar el runner local portable.
