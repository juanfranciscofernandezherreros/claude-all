# BACKLOG — Kafka Performance Lab

Este backlog es el plan obligatorio de implementación. Ejecutar en orden; las dependencias son bloqueantes. PRD.md define el contrato funcional y prevalece ante detalles de negocio. No añadir tecnologías ni alcance. Cada tarea se cierra solo con sus evidencias; actualizar su estado en este archivo. Estados iniciales: TODO.

## Reglas de ejecución
- Orden: fases B00–B10. No empezar una fase si fallan sus dependencias, salvo trabajo independiente.
- Por tarea: implementar -> ejecutar validación indicada -> corregir -> anotar evidencia breve (comando y resultado).
- Commits pequeños por fase. No generar documentación duplicada: README para uso; PRD para requisitos; este archivo para tareas.
- Mantener el consumo de Claude bajo: leer PRD y BACKLOG una vez al inicio; después consultar solo archivos relevantes. No imprimir archivos completos ni narrar operaciones. Resumen final con evidencias y bloqueos.
- No marcar DONE por inspección visual. No inventar pruebas, cifras ni hardware.
- Si falta una decisión técnica menor, aplicar la opción más simple compatible con PRD, documentarla en ADR breve solo si afecta arquitectura; no preguntar.

## Convenciones y layout objetivo
Maven multi-módulo NO: un único proyecto Maven para reducir complejidad. Paquete base `com.kafkalab`.
Directorios: `src/main/java`, `src/main/resources`, `src/test/java`, `scripts/`, `monitoring/prometheus/`, `monitoring/grafana/provisioning/`, `benchmark-results/`.
Clases separadas por responsabilidad: config, model, generator, producer, consumer, processing, validation, benchmark, metrics, api.
No implementar UI propia; Grafana es la interfaz de métricas. App REST mínima: health/info y control de ejecución si resulta necesario; benchmark controlado por CLI/scripts.
RunId: UUID por ejecución; messageId determinista `<runId>-<sequence>` (string, único). Este formato prevalece sobre la palabra UUID aislada del campo messageId del PRD.

## Backlog ordenado

### B00 — Inspección y contrato
**Depende:** nada. **Salida:** estructura existente entendida y decisiones consistentes.
- [ ] B00.1 Inspeccionar repo, herramientas, Docker, Java, sistema operativo y archivos existentes; no borrar trabajo ajeno.
- [ ] B00.2 Verificar coherencia PRD: messageId string `runId-sequence`; tiempos UTC en mensajes y monotónicos en medición; diferencia entre procesamiento y end-to-end.
- [ ] B00.3 Crear árbol Maven indicado y .gitignore para target, logs, volúmenes y resultados masivos; conservar solo informes agregados pequeños.
**Aceptación:** inventario y árbol creados; no hay decisiones abiertas ni contradicciones en contrato.

### B01 — Proyecto y configuración
**Depende:** B00.
- [ ] B01.1 Crear pom.xml con Java 21, Spring Boot 3.4.5, Spring Kafka, Actuator/Micrometer, Prometheus registry, Jackson, JUnit 5 y Testcontainers; fijar versiones compatibles.
- [ ] B01.2 Añadir Maven Wrapper 3.9.9 y configuración reproducible; compilar app mínima.
- [ ] B01.3 Crear application.yml y binding tipado de variables del PRD; defaults, validación de rangos y fallo temprano con mensaje claro.
- [ ] B01.4 Crear .env.example sin secretos y documentación de todas las variables.
**Aceptación:** `./mvnw clean verify` y `mvnw.cmd clean verify` funcionan; configuración inválida falla con código distinto de cero.

### B02 — Kafka local reproducible
**Depende:** B01.
- [ ] B02.1 Crear Dockerfile multi-stage reproducible para app.
- [ ] B02.2 Crear compose.yaml con kafka KRaft, app, Prometheus y Grafana; versiones fijas, listeners interno/externo, volumen persistente y puertos PRD.
- [ ] B02.3 Configurar healthcheck Kafka verificando broker realmente disponible; dependencias condicionadas a healthy.
- [ ] B02.4 Crear topics input/output/DLT con 24 particiones y RF=1 idempotentemente al iniciar.
- [ ] B02.5 Provisionar Prometheus y Grafana automáticamente, sin pasos manuales.
**Aceptación:** `docker compose up -d --build`; todos healthy; host conecta a localhost:9094; contenedores conectan a kafka:9092; topics exactos presentes; reinicio conserva volumen.

### B03 — Contrato de dominio
**Depende:** B01.
- [ ] B03.1 Crear DTOs versionados de entrada, salida y DLT; validaciones obligatorias y timestamps UTC.
- [ ] B03.2 Crear fábrica de IDs `runId-sequence` y checksum SHA-256 UTF-8 determinista.
- [ ] B03.3 Implementar LIGHT/MEDIUM/HEAVY con 1/10/100 rondas SHA-256 sin sleep.
- [ ] B03.4 Añadir tests de serialización, validación, IDs, checksum y tamaños de payload.
**Aceptación:** tests cubren límites, JSON inválido, determinismo e igualdad de ID/sequence.

### B04 — Productor y generador
**Depende:** B02, B03.
- [ ] B04.1 Configurar producer con acks=all, idempotencia, retries y batch/linger/compression configurables.
- [ ] B04.2 Implementar generador CLI que publique N mensajes únicos; threads configurables, payload exacto, progreso cada 10.000, runId y métricas de publicación.
- [ ] B04.3 Esperar confirmación de todos los envíos; fallo de envío produce exit code no cero y registro.
- [ ] B04.4 Tests de generación, distribución por partición, tamaño, unicidad y error de publicación.
**Aceptación:** publicar 10.000 entradas; todas confirmadas y IDs únicos; tiempo de publicación medido aparte.

### B05 — Consumer, transformación y errores
**Depende:** B02, B03, B04.
- [ ] B05.1 Implementar consumer con group kafka-lab, concurrencia y batch/poll configurables.
- [ ] B05.2 Procesar, transformar y publicar una salida por entrada válida; preservar messageId y sequence.
- [ ] B05.3 AT_LEAST_ONCE: confirmar offset solo tras ACK de salida; reintentos 3 y backoff 1/2/4s (cap 10s); después DLT.
- [ ] B05.4 Errores de validación directos a DLT; DLT incluye entrada original, tipo/mensaje de error y timestamp.
- [ ] B05.5 Manejo de shutdown y excepciones para no confirmar prematuramente.
**Aceptación:** prueba e2e de 10.000 con 0 pérdidas; error inválido en DLT; reinicio sin pérdida silenciosa; duplicados posibles documentados.

### B06 — EXACTLY_ONCE y resiliencia
**Depende:** B05.
- [ ] B06.1 Configurar transacciones Kafka, transactional.id único por instancia y read_committed.
- [ ] B06.2 Publicar salida y confirmar offsets en una transacción atómica.
- [ ] B06.3 Añadir pruebas de commit, abort/rollback, reinicio durante transacción y aislamiento read_committed.
- [ ] B06.4 Verificar recuperación/retry/DLT sin duplicación lógica en modo EOS.
**Aceptación:** tests reproducibles prueban rollback y atomicidad Kafka; no afirmar EOS en generador ni sistemas externos.

### B07 — Métricas y dashboard
**Depende:** B04, B05.
- [ ] B07.1 Registrar contadores/timers del PRD y percentiles p50/p95/p99.
- [ ] B07.2 Exponer Actuator/Prometheus; excluir IDs de labels.
- [ ] B07.3 Configurar scrape de app y dashboard Grafana provisionado con throughput, latencia, errores, DLT, lag y JVM.
**Aceptación:** Prometheus scrapea app; métricas aumentan durante e2e; dashboard se carga sin configuración manual.

### B08 — Validador independiente
**Depende:** B04, B05.
- [ ] B08.1 Crear validador separado del consumer que recorra output-topic con read_committed cuando aplique.
- [ ] B08.2 Comparar set esperado de IDs vs outputs con memoria acotada (archivo/DB temporal o algoritmo streaming); detectar missing, duplicate, unexpected, invalid y DLT.
- [ ] B08.3 Emitir validation.json y código de salida no cero si integridad incumple criterios.
- [ ] B08.4 Añadir tests con datasets de pérdida, duplicado, extra y corrupto.
**Aceptación:** resultados conocidos detectados exactamente; 1M no requiere cargar todos los mensajes en RAM.

### B09 — Orquestación y benchmark
**Depende:** B06, B07, B08.
- [ ] B09.1 Implementar runner por runId: limpiar estado de topics/offsets de forma segura, warm-up 10k y 3 repeticiones medidas.
- [ ] B09.2 Implementar medición monotónica publish/processing/end-to-end; throughput solo outputs únicos validados; latencias min/mean/p50/p95/p99/max.
- [ ] B09.3 Implementar suite principal 1M por concurrency 1/2/5/10/20 y suites --full-suite de payload/workload/delivery según PRD.
- [ ] B09.4 Guardar summary.json, results.csv, validation.json, environment.json, execution.log y report.md por run.
- [ ] B09.5 Capturar CPU/memoria/heap/GC/lag cuando disponibles; anotar ausencias.
- [ ] B09.6 Implementar stress opcional con condiciones de parada exactas del PRD.
- [ ] B09.7 Asegurar limpieza aislada entre runs; no borrar volúmenes/datos de usuario ni resultados anteriores.
**Aceptación:** benchmark 1M ejecuta o deja blocker inequívoco; cifras reproducibles; cada run contiene todos los artefactos; datos validados antes de calcular throughput.

### B10 — Experiencia Windows/Linux, documentación y cierre
**Depende:** B02, B09.
- [ ] B10.1 Crear scripts start/stop/benchmark/full-suite/validate para Windows y shell.
- [ ] B10.2 Actualizar RUN-KAFKA-LAB.bat: comprobar Git/Claude/Docker, iniciar Docker Desktop si existe, esperar disponibilidad con timeout, lanzar Claude una sola vez y devolver exit code.
- [ ] B10.3 Documentar prerequisitos, comandos, puertos, variables, troubleshooting y formatos de resultados en README.
- [ ] B10.4 Ejecutar mvnw clean verify, smoke e2e, benchmark 1M, validación independiente y --full-suite solo si se solicita.
- [ ] B10.5 Revisar git diff, eliminar secretos/artefactos temporales, verificar scripts y dejar informe agregado reproducible.
**Aceptación final:** PRD Definition of Done satisfecho; instrucciones funcionan desde clon limpio; informe final distingue ejecutado, fallido y bloqueado.

## Grafo resumido
B00 -> B01 -> B02 -> B04 -> B05 -> B06 -> B09 -> B10
B01 -> B03 -> B04
B04+B05 -> B07
B04+B05 -> B08
B06+B07+B08 -> B09

## Protocolo de cierre por tarea
En cada checkbox sustituir TODO por DONE solo tras pasar aceptación; si no, BLOCKED con causa y evidencia. Registrar en una línea: `ID | estado | comando/prueba | resultado`. No pegar logs extensos en este archivo: guardar logs bajo benchmark-results. Si cambia alcance/arquitectura, actualizar primero PRD y este backlog en el mismo commit.

## Criterio global
No existe DONE global por tener código escrito. Solo al cumplir B10.5 con evidencias reales. Si el entorno impide ejecutar una prueba, marcarla BLOCKED, explicar el requisito externo y completar el resto.
