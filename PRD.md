# Kafka Performance Lab — Contrato ejecutable

## 1. Objetivo y reglas
Construir, ejecutar y medir un pipeline Java/Kafka local: publicar 1.000.000 entradas, procesarlas y producir exactamente una salida lógica por entrada válida. Entregar código funcional, pruebas, benchmark real y evidencias. No entregar solo un plan.
Decisiones cerradas: Java 21; Spring Boot 3.4.5; Spring Kafka gestionado por Spring Boot; Maven Wrapper 3.9.9; JUnit 5; Testcontainers; Apache Kafka 3.9.0 en Docker; KRaft sin ZooKeeper; JSON; Micrometer + Prometheus + Grafana. Fijar todas las versiones e imágenes, nunca usar latest. Si una versión fijada no está disponible, no sustituir silenciosamente: documentar bloqueo.
No preguntar ni solicitar aprobaciones rutinarias. No inventar resultados. No modificar archivos ajenos al proyecto.

## 2. Arquitectura fija
Servicios Compose: kafka (un broker KRaft, 24 particiones, replication factor 1), app (Spring Boot), prometheus y grafana. Kafka listener interno kafka:9092 y externo localhost:9094. Healthcheck real; app espera broker healthy. Compose crea input-topic, output-topic e input-dlt-topic al arrancar; retención suficiente para benchmark. Persistir datos en volumen. Puertos: Kafka 9094, app 8080, Prometheus 9090, Grafana 3000.
Flujo: generador independiente -> input-topic -> consumer -> transformación -> producer -> output-topic -> validador independiente. Consumer y producer en la misma aplicación. Configuración externa por variables de entorno.
Particionar por messageId. Grupo kafka-lab. Concurrencia configurable. Serialización JSON con esquema versionado.

## 3. Contrato de mensajes
Entrada: {schemaVersion:1,messageId:string(runId-sequence),createdAt:ISO-8601 UTC,eventType:"BENCHMARK",payload:{sequence:long,data:string}}. IDs únicos. Salida: {schemaVersion:1,messageId,createdAt,processedAt,status:"OK",result:{sequence,checksum}}. Transformación determinista: SHA-256 UTF-8 de data; conservar sequence e ID. No usar reloj/azar en resultado.
Una salida por cada entrada válida. Confirmar offset solo después de confirmar publicación de salida. AT_LEAST_ONCE puede duplicar tras fallo; validador detecta duplicados. EXACTLY_ONCE usa transacción Kafka para publicar salida y confirmar offsets atómicamente; producer transaccional por instancia, isolation.level=read_committed. No prometer atomicidad fuera de Kafka.
JSON inválido o fallo permanente -> input-dlt-topic con payload original, error y timestamp; no descartar silenciosamente. Reintentos: 3, backoff 1s exponencial x2, máximo 10s; después DLT. Errores transitorios recuperables reintentan; errores de validación van directos a DLT.

## 4. Configuración predeterminada
MESSAGES=1000000; PARTITIONS=24; CONCURRENCY=1; BATCH_SIZE=500; PRODUCER_BATCH_BYTES=65536; GENERATOR_THREADS=4; PAYLOAD_BYTES=1024; DELIVERY_MODE=AT_LEAST_ONCE; WORKLOAD=LIGHT; COMPRESSION_TYPE=none; LINGER_MS=5. BATCH_SIZE significa máximo de registros por poll del consumer (max.poll.records) y tamaño de bloque lógico del generador; NO representa bytes. Kafka producer batch.size se configura en bytes mediante PRODUCER_BATCH_BYTES. Variables documentadas y validadas al iniciar. Concurrencia permitida: 1,2,5,10,20. Payload: 1,5,10,50,100 KiB. WORKLOAD: LIGHT=solo SHA-256; MEDIUM=10 rondas SHA-256; HEAVY=100 rondas SHA-256. Sin sleeps artificiales. No cambiar parámetros durante una ejecución.
Generador: runId UUID por ejecución; messageId determinista string <runId>-<sequence>, único y reproducible, tasa ilimitada por defecto, progreso cada 10.000, fallos y duración de publicación separados.

## 5. Métricas
Micrometer/Prometheus: input_received_total, output_published_total, processing_failed_total, retry_total, dlt_total, processing_duration, end_to_end_latency, publish_duration, consumer_lag, JVM heap/GC y CPU/memoria si Docker los expone. Histogramas con percentiles p50/p95/p99. Nunca usar IDs como labels. Dashboard Grafana provisionado automáticamente.

## 6. Medición y matriz
Usar reloj monotónico para duraciones. Definiciones: publish_seconds = fin de publicación de entradas - inicio; processing_seconds = fin de procesamiento - inicio de consumo; end_to_end_seconds = instante en que validador confirma todas las salidas - inicio de publicación. throughput = salidas válidas únicas / end_to_end_seconds. Latencia = processedAt - createdAt; informar min/mean/p50/p95/p99/max. Error rate = entradas fallidas / entradas totales. No contar consumos ni duplicados como throughput.
Cada escenario: una precarga/warm-up de 10.000 mensajes no contabilizada y 3 ejecuciones medidas; reportar mediana y cada ejecución. Vaciar topics/resetear offsets y usar runId distinto antes de cada ejecución.
Suite principal: 1.000.000 mensajes, 1 KiB, LIGHT, AT_LEAST_ONCE; concurrencia 1,2,5,10,20.
Suite payload: 100.000 mensajes, concurrencia 5, LIGHT, AT_LEAST_ONCE; tamaños 1,5,10,50,100 KiB.
Suite workload: 100.000 mensajes, concurrencia 5, 1 KiB, AT_LEAST_ONCE; LIGHT/MEDIUM/HEAVY.
Suite delivery: 100.000 mensajes, concurrencia 5, 1 KiB, LIGHT; AT_LEAST_ONCE y EXACTLY_ONCE.
Ejecutar suites secundaria solo con --full-suite; benchmark por defecto ejecuta suite principal. --full-suite ejecuta todas. Stress opcional: aumentar carga por pasos de 100.000 hasta 5.000.000; parar si errores >0, lag crece 3 pasos consecutivos o memoria >85%.
No cambiar configuración ni descartar ejecuciones lentas. Registrar hardware, SO, Docker, versiones, configuración y hora.

## 7. Validación, pruebas y aceptación
Validador independiente y acotado en memoria (streaming o almacenamiento temporal) compara IDs esperados/reales; contar missing, duplicate, unexpected, invalid, failed y DLT. Para escenario funcional válido: 0 missing, 0 duplicate, 0 unexpected; total de salidas válidas = entradas válidas. Fallos deben estar explicados y contabilizados.
JUnit unit tests: esquema, validación, checksum, errores, configuración. Testcontainers integration: publish/consume, offsets, retry, DLT, transacción, rollback y reinicio. E2E local 1.000.000.
Objetivo orientativo, no condición de éxito: end-to-end <60s (~16.667 salidas/s). Informar resultado real y limitaciones de hardware; no falsear PASS si no se ejecutó.

## 8. Entregables y comandos
Proyecto Maven con código, Dockerfile, compose.yaml, .env.example, Maven Wrapper, pruebas, scripts y documentación. Scripts: start, stop, benchmark, full-suite, validate. Windows: RUN-KAFKA-LAB.bat; shell scripts equivalentes. Comandos estables: mvnw.cmd clean verify; RUN-KAFKA-LAB.bat; scripts/run-benchmark.bat --messages 1000000; scripts/run-full-suite.bat.
Cada run en benchmark-results/<runId>/: summary.json, results.csv, validation.json, environment.json, execution.log, report.md. No versionar resultados temporales grandes; incluir informe agregado y CSV pequeño.
README: prerequisitos exactos, arranque/parada, comandos Windows/Linux, puertos, variables, troubleshooting, interpretación de métricas y ubicación de informes.

## 9. Ejecución autónoma / Definition of Done
Inspeccionar entorno y repositorio. Construir todo, iniciar Docker Desktop si está instalado pero parado (Windows launcher), arrancar Compose, esperar healthcheck, crear topics, compilar, probar, ejecutar 1M, validar independientemente, ejecutar suite solicitada, corregir y repetir. Optimizar solo tras guardar baseline; cambiar una variable por iteración y medir de nuevo. Continuar tareas independientes ante bloqueos y anotar bloqueo exacto.
DoD: build exitoso; Kafka local saludable; flujo generador-consumidor-productor; tests ejecutados; benchmark de 1M ejecutado o bloqueo concreto; validación independiente; informes reproducibles; README completo.
Respuesta final breve: comandos, pruebas ejecutadas, recuentos, tiempos, throughput, latencias, recursos, integridad, mejor configuración, comparación baseline/optimizado y bloqueos.