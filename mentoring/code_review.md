# Revisión de `bad_code.py`

## Observaciones accionables

1. **El procesamiento abandona Spark.** `pandas.read_csv` carga el archivo completo en la memoria del driver y `iterrows()` procesa una fila por vez. Esto limita el volumen y desperdicia el motor distribuido. Debe leerse con `spark.read`, expresar filtros y cálculos mediante funciones de Spark y ejecutar una única transformación distribuida.

2. **Las reglas de negocio están incrustadas en el control de flujo.** El país, los tipos de entrega y el factor `20` no provienen de configuración. Cada cambio exige editar y desplegar código. Deben modelarse como parámetros validados y, cuando corresponda, como configuración jerárquica por tenant.

3. **La conversión acepta silenciosamente unidades desconocidas.** Toda unidad distinta de `CS` se trata como `ST`, por lo que un error como `EA` genera métricas falsas. La transformación debe aceptar explícitamente `CS` y `ST`, y enviar las demás unidades a cuarentena o provocar un error controlado.

4. **La escritura no implementa la idempotencia requerida.** `mode("overwrite")` sobre una ruta concatenada puede eliminar más datos de los previstos y no expresa qué partición se reemplaza. Se debe usar Delta Lake y `replaceWhere` para Bronze/Gold, o `MERGE` con una clave de negocio explícita para Silver.

5. **No hay contrato de esquema ni controles de calidad.** La inferencia implícita entre pandas y Spark puede cambiar tipos según el archivo; tampoco se controlan nulos, cantidades no positivas o fechas inválidas. Se requieren esquemas explícitos, cuarentena y resultados de calidad persistidos.

6. **El módulo ejecuta trabajo durante el import.** La última línea dispara el pipeline al importar el archivo, lo que dificulta tests y reutilización. La ejecución debe estar detrás de una CLI o de `if __name__ == "__main__"`, mientras que las transformaciones deben ser funciones puras testeables.

7. **La observabilidad es insuficiente.** `print("done")` no identifica corrida, tenant, conteos ni fallos. Deben emitirse logs estructurados con `run_id`/`batch_id`, métricas de entrada, salida, cuarentena y una excepción con contexto cuando la ejecución falle.

## Cómo se lo explicaría al junior

Empezaría separando la conversación sobre la persona de la conversación sobre el código: el objetivo no es “usar Spark porque sí”, sino conservar el procesamiento dentro del motor que puede escalar, reintentarse y auditarse. Recorreríamos juntos una regla —la conversión de cajas— y la expresaríamos primero como transformación declarativa. Después agregaríamos un caso inválido y observaríamos por qué una rama `else` demasiado amplia corrompe datos silenciosamente.

El feedback se daría en iteraciones pequeñas: primero corrección funcional y esquema; luego idempotencia; finalmente configuración, tests y observabilidad. Le pediría investigar evaluación lazy de Spark, diferencias entre transformaciones y acciones, particionado de Delta Lake y semántica de `MERGE`/`replaceWhere`. La siguiente entrega debería incluir tests para `CS`, `ST`, unidad desconocida, cantidad negativa y reejecución del mismo tenant. Así la mejora se mide con comportamiento verificable, no solo con cambios de estilo.
