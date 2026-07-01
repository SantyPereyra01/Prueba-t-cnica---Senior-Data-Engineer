# Guion de sustentación (30 minutos)

## Demo — 10 minutos

1. Mostrar configuración jerárquica y ejecutar un tenant/rango pequeño.
2. Enseñar el JSON final y paths separados por tenant.
3. Consultar un ejemplo SCD2 donde marzo y abril usen versiones distintas.
4. Mostrar cuarentena y `quality_logs` con `run_id`.
5. Reejecutar el rango y comparar conteos para demostrar idempotencia.

## Walkthrough — 10 minutos

- Bronze: esquema explícito, columnas técnicas y `replaceWhere`.
- Silver: deduplicación, unidad común, clasificación de anomalías, MERGE y join temporal.
- Gold: precio transaccional, granularidad y recomputación por rango.
- Multi-tenant: configuración, paths y comportamiento `fail_fast`.
- Tests: casos de conversión, descarte, cuarentena y SCD histórico.

## Observaciones — 5 minutos

Priorizar la clave de negocio insuficiente, la semántica de deletes y la partición redundante. Explicar qué se respetó, qué riesgo queda y qué contrato adicional resolvería cada punto.

## Preguntas — 5 minutos

Preparar respuestas sobre small files, evolución de esquema, concurrencia sobre `quality_logs`, solapamientos SCD2, recuperación de fallos parciales y migración de paths locales a Unity Catalog.
