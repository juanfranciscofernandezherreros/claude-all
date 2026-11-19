# BACKLOG — Kafka Performance Lab

Este backlog es el plan obligatorio de implementación. Ejecutar en orden; las dependencias son bloqueantes. PRD.md define el contrato funcional y prevalece ante detalles de negocio. No añadir tecnologías ni alcance. Cada tarea se cierra solo con sus evidencias; actualizar su estado en este archivo. Estados: DONE solo con verificación ejecutada; DONE — pendiente de verificación cuando la implementación está escrita pero falta entorno; BLOCKED cuando falta implementación o una prueba necesaria.


## Regla obligatoria de checkbox (no omitir)
- Actualizar `BACKLOG.md` inmediatamente al completar cada tarea o micro-paso y hacer commit visible de esa actualización; nunca acumular los cambios de estado para el final.
- `[x]` significa que la implementación de la tarea está completada y registrada. También se marca `[x]` si solo falta una verificación que no puede ejecutarse en el entorno actual; en ese caso escribir `DONE — pendiente de verificación` y especificar la prueba pendiente.
- `[ ]` significa que la tarea sigue incompleta o está bloqueada porque falta implementación o trabajo necesario; escribir `BLOCKED — causa concreta`.
- Escribir `DONE` sin calificativo solo después de ejecutar y superar la prueba de aceptación.
- No basta con cambiar la etiqueta de estado: **la casilla debe cambiarse siempre**. Antes de cerrar cada sesión, revisar que ninguna tarea completada durante ella siga con `[ ]`.

## Nivel obligatorio de detalle
Cada ID de tarea es una unidad ejecutable, no una sugerencia. Antes de implementarla, Claude debe:
1. Leer solo los archivos implicados y comprobar dependencias DONE.
2. Ejecutar los pasos concretos enumerados debajo, en orden.
3. Crear/modificar únicamente los artefactos indicados por la tarea.
4. Ejecutar la prueba indicada y comprobar el resultado esperado.
5. Marcar DONE solo si pasa; si no, corregir o marcar BLOCKED con causa, nunca saltar silenciosamente.
6. Registrar evidencia corta: comando, resultado observado y archivos afectados.

Una tarea sin pasos, artefactos y prueba no está suficientemente definida: desglosarla en este mismo archivo antes de codificarla. El trabajo de Claude puede agruparse para ahorrar tokens, pero los cambios Git deben seguir siendo commits atómicos por paso verificable.

## Reglas de ejecución
- Orden: fases B00–B10. No empezar una fase si fallan sus dependencias, salvo trabajo independiente.
- Por tarea: implementar -> ejecutar validación indicada -> corregir -> anotar evidencia breve (comando y resultado).
- **Granularidad Git obligatoria:** un commit por cada paso pequeño que produzca un cambio real en archivos. Si un punto de “Pasos” contiene varias acciones independientes, dividirlo en commits atómicos adicionales. No agrupar pasos distintos en un único commit.
- Cada commit debe incluir solo los archivos del paso y usar Conventional Commits con ID de tarea: `feat(B04.2): add deterministic message ID generator`, `test(B04.2): verify ID uniqueness`, `docs(B04.2): document generator flags`, `fix(B05.3): commit offsets after output ack`.
- Antes de cada commit: revisar `git diff --check`, ejecutar la prueba más específica disponible y comprobar `git status`. No crear commits vacíos. Las comprobaciones que no cambien archivos se anotan como evidencia, sin commit artificial.
- Mantener historial lineal y legible: no squash, no rebase interactivo, no amend de commits ya publicados y no force-push. No mezclar tareas distintas.
- Si un cambio intermedio depende de otro para compilar, hacer el menor commit vertical que deje el proyecto compilable; documentar la dependencia. Al cerrar cada tarea, actualizar su checkbox y evidencia en un commit propio si no quedó incluida en el último commit funcional.
- No generar documentación duplicada: README para uso; PRD para requisitos; este archivo para tareas y evidencias.
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
- [ ] B00.1 **BLOCKED — inventario del hardware local.** El repositorio ya contiene Maven, Java, Docker Compose, tests y scripts; GitHub Actions valida build y runtime en Ubuntu. Lo único no verificable desde GitHub es el equipo objetivo del usuario: Windows/macOS/Linux, CPU, RAM, versión de Java/Docker Desktop y almacenamiento. Registrar esos datos cuando se ejecute desde el host objetivo; no confundirlos con el runner de CI.
  - Pasos: comprobar rama/estado Git; listar archivos; comprobar java -version, docker version/info, docker compose version, git --version y claude --version; identificar SO/arquitectura/CPU/RAM; registrar faltantes.
  - Artefactos: inventario breve en BACKLOG (evidencia), sin volcar logs.
  - Prueba: inventario remoto completado mediante GitHub API; comprobación local de Java/Docker/CPU/RAM pendiente. No marcar DONE hasta completar la inspección local.
- [x] B00.2 **DONE.** Revisados esquemas, IDs, modos de entrega, reintentos, fórmulas, suites y variables. Corregida ambigüedad de batching: `BATCH_SIZE=500` son registros por poll/bloque lógico; `PRODUCER_BATCH_BYTES=65536` son bytes para Kafka producer. Evidencia: commit PRD `f8d9082`.
  - Pasos: contrastar esquemas entrada/salida/DLT; comprobar reglas EOS/at-least-once; confirmar fórmulas, escenarios y variables; corregir contradicciones en PRD antes del código.
  - Artefactos: PRD corregido solo si existe contradicción real.
  - Prueba: revisar que backlog y PRD usan mismos nombres, valores y criterios.
- [x] B00.3 **DONE — pendiente inspección del árbol en checkout del usuario.** `.gitignore` cubre Maven target, secretos `.env` (excepto `.env.example`), logs, datos Kafka/Docker, offsets, warm-up, validación temporal, muestras de recursos y scrapes Prometheus (`43462c2`, `428b361b`). CI `44538143950` ejecutó `git check-ignore` sobre target, `.env`, logs, offsets, resource samples, Prometheus raw y kafka-data, y confirmó que `.env.example` sigue trackeable.
  - Pasos: crear directorios del layout; ignorar target, .env, logs, datos Kafka, temporales y resultados de gran tamaño; permitir .env.example y resumen de benchmark.
  - Artefactos: árbol base, .gitignore.
  - Prueba: archivo `.gitignore` revisado; `git check-ignore` y `git status` pendientes en entorno local.
**Aceptación:** inventario y árbol creados; no hay decisiones abiertas ni contradicciones en contrato.

### B01 — Proyecto y configuración
**Depende:** B00.
- [x] B01.1 **DONE.** `pom.xml` configura Spring Boot 3.4.5, Java 21 y dependencias requeridas; CI `44537286874` superó `./mvnw clean verify` y Docker build.
  - Pasos: usar parent/BOM Spring Boot; incluir starter web, kafka, actuator, validation, micrometer-registry-prometheus, Jackson, test starter y Testcontainers Kafka; fijar plugin compiler/surefire; evitar dependencias redundantes.
  - Artefactos: pom.xml.
  - Prueba: mvn dependency:tree sin conflictos; compiler release 21.
- [x] B01.2 **DONE — wrapper Windows pendiente de prueba nativa.** `mvnw`/`mvnw.cmd`, `KafkaPerformanceLabApplication` y workflow compilados/ejecutados; CI `44537286874` superó `clean verify`, build Docker y health UP. Falta probar `mvnw.cmd` en Windows.
  - Pasos: generar wrapper oficial; comprobar scripts Unix/Windows y wrapper jar/properties; crear clase main Spring Boot y contexto mínimo.
  - Artefactos: mvnw, mvnw.cmd, .mvn/wrapper/*, clase de arranque.
  - Prueba: ./mvnw -version, mvnw.cmd -version y clean package.
- [x] B01.3 **DONE.** `LabProperties` y validaciones de rangos/concurrencia cargan al iniciar; CI `44537286874` levantó app, health UP y smoke e2e PASS de tres repeticiones.
  - Pasos: agrupar propiedades kafka/app/benchmark/metrics; mapear cada variable PRD (incluidos BATCH_SIZE y PRODUCER_BATCH_BYTES); validar enum/rango/positividad; no registrar secretos; documentar defaults.
  - Artefactos: application.yml, clases @ConfigurationProperties y tests.
  - Prueba: contexto arranca con defaults; valores inválidos impiden arrancar con mensaje accionable.
- [x] B01.4 **DONE.** README documenta todas las variables Compose/Spring con defaults, rangos, unidades y efectos (`c0666d0` y actualización posterior); `.env.example` contiene valores alineados. CI `44537286874` ejecutó `docker compose config --quiet` correctamente; el stack real arrancó con health UP (`44537310704`).
  - Pasos: listar variable, default, rango, unidad y efecto; mantener Compose y Spring con nombres idénticos; no incluir credenciales reales.
  - Artefactos: .env.example y tabla de variables en README.
  - Prueba: Compose interpola el ejemplo y app arranca con configuración por defecto.
**Aceptación:** `./mvnw clean verify` y `mvnw.cmd clean verify` funcionan; configuración inválida falla con código distinto de cero.

### B02 — Kafka local reproducible
**Depende:** B01.
- [x] B02.1 **DONE.**
  - Pasos: etapa build con JDK 21 y Maven Wrapper; etapa runtime JRE 21 no-root; copiar solo artefacto; healthcheck app si procede; fijar imagen base.
  - Artefactos: Dockerfile y .dockerignore.
  - Prueba: docker build finaliza y contenedor arranca con usuario no-root.
- [x] B02.2 **DONE — pendiente de CI con prueba externa/restart.** Compose expone Kafka `localhost:9094`, usa KRaft/volumen persistente y app health UP; CI añade cliente desde host (`--network host`), reinicio broker, verificación de topics y reinicio app para probar recuperación (`c72870dd`).
  - Pasos: configurar node.id/roles/controller quorum/listeners/advertised listeners; definir red, volumen, restart policy, límites razonables y puertos; nunca latest.
  - Artefactos: compose.yaml y configuración Kafka.
  - Prueba: docker compose config válida; cliente desde host y contenedor resuelve listeners correctos.
- [x] B02.3 **DONE — healthcheck configurado; pendiente prueba de fallo.**
  - Pasos: usar comando kafka-broker-api-versions o equivalente incluido en imagen; definir interval/timeout/retries/start_period; app depende de service_healthy.
  - Artefactos: healthcheck y depend_on en compose.
  - Prueba: arranque frío pasa de starting a healthy; detener broker provoca unhealthy.
- [x] B02.4 **DONE — pendiente de CI de idempotencia.** `KafkaTopicConfiguration` crea `input-topic`, `output-topic`, `input-dlt-topic` declarativamente con 24 particiones; CI reinicia broker y app y exige que los tres topics sigan presentes (`c72870dd`).
  - Pasos: script/init container espera broker; crea topics con nombres exactos; si ya existen, valida configuración y no falla; definir retención para suite.
  - Artefactos: init script/servicio y definición de topics.
  - Prueba: dos arranques seguidos sin error; describe topics confirma particiones/replication factor.
- [x] B02.5 **DONE.** CI `44537286874` y `44537310704` levantó Prometheus 3.2.1 y Grafana 11.6.0 reales; endpoints `/-/ready` y `/api/health` respondieron, API de Grafana confirmó datasource Prometheus en `http://prometheus:9090` y dashboard UID `kafka-performance-lab` (`98caca27`).
  - Pasos: crear prometheus.yml scrape app; datasource provisioning; dashboard provisioning; credenciales locales no sensibles y documentadas.
  - Artefactos: monitoring/prometheus/* y monitoring/grafana/provisioning/*.
  - Prueba: targets Prometheus UP; dashboard visible tras arrancar Compose.
**Aceptación:** `docker compose up -d --build`; todos healthy; host conecta a localhost:9094; contenedores conectan a kafka:9092; topics exactos presentes; reinicio conserva volumen.

### B03 — Contrato de dominio
**Depende:** B01.
- [x] B03.1 **DONE. Tests de contrato JSON para OutputMessage/DltMessage verifican campos estables, nested result, checksum, original payload, metadata de error y round-trip (`10a1d1fd`, fix de nombres `7faf0984`); CI `44537093501` superó `clean verify`.**
  - Pasos: declarar campos/tipos exactos del PRD; Bean Validation para null/vacíos/rangos; UTC ISO-8601; schemaVersion=1; preservar entrada original en DLT.
  - Artefactos: model/InputMessage, OutputMessage, DltMessage y tests.
  - Prueba: serialización JSON golden tests y rechazo de cada campo obligatorio inválido.
- [x] B03.2 **DONE.** Unit tests de checksum, IDs, workload y contrato ejecutados por `clean verify`; CI `44537286874` superó compilación y tests sin fallos. Cobertura de bordes ampliada en B03.3/B03.4.
  - Pasos: generar UUID por run; construir ID con secuencia decimal estable; checksum bytes UTF-8 en hexadecimal; evitar dependencia del reloj.
  - Artefactos: RunIdProvider, MessageIdFactory, ChecksumService.
  - Prueba: vectores SHA-256 conocidos; misma entrada produce mismo ID/checksum; secuencias no colisionan.
- [x] B03.3 **DONE.** CI `44538795072` superó `./mvnw -B clean verify`, incluidos vectores golden SHA-256 exactos para 1/10/100 rondas LIGHT/MEDIUM/HEAVY. Vectores calculados independientemente y corregidos (`730a3a2a`); test añadido (`385ef18a`).
  - Pasos: parsear enum; aplicar exactamente N rondas sobre bytes de resultado anterior; evitar optimización que elimine trabajo; no introducir pausas.
  - Artefactos: WorkloadProcessor y tests.
  - Prueba: outputs deterministas y coste creciente comprobable sin exigir tiempos absolutos.
- [x] B03.4 **DONE.** CI `44532639846` superó `clean verify`; incluidos round-trip JSON/Instant y Unicode (`café-東京-🙂`), JSON corrupto, secuencia mínima 0 y límites payload 1 KiB/100 KiB en UTF-8 (`90cacb1c`).
  - Pasos: cubrir casos válidos, campos ausentes, JSON corrupto, Unicode, payload mínimo/máximo y límites configurados.
  - Artefactos: tests unitarios JUnit.
  - Prueba: mvnw -Dtest=*Model*,*Checksum*,*MessageId* test (ajustar patrones a nombres reales).
**Aceptación:** tests cubren límites, JSON inválido, determinismo e igualdad de ID/sequence.

### B04 — Productor y generador
**Depende:** B02, B03.
- [x] B04.1 **DONE.** Producer Kafka configurado con bootstrap externo/interno, key=messageId, JSON serializer, acks=all, idempotencia, retries, batch bytes, linger y compresión. CI `44537286874` levantó stack y smoke de tres repeticiones PASS; generación/ACK reales confirmados.
  - Pasos: definir bootstrap según entorno; key=messageId; serializer JSON; acks all; enable.idempotence true; retries compatibles; batch.size=PRODUCER_BATCH_BYTES; linger/compression externos.
  - Artefactos: KafkaProducerConfig y propiedades.
  - Prueba: inspeccionar configuración efectiva y publicar/leer mensaje de prueba.
- [x] B04.2 **DONE — pendiente de verificación funcional.** Generador por bloques con envíos asíncronos y ACK por bloque; EXACTLY_ONCE agrupa cada bloque en transacción. CI `bf6acc9` compila el código; faltan prueba 10k, payloads, IDs únicos y rendimiento.
  - Pasos: validar argumentos; crear runId; generar secuencias 1..N sin duplicar; construir payload con bytes exactos; agrupar envío en bloques de BATCH_SIZE registros; distribuir envío entre threads; usar callbacks/acks.
  - Artefactos: generator CLI, payload factory y documentación de flags.
  - Prueba: generar 10.000; contar IDs distintos y comprobar payload bytes y secuencia continua.
- [x] B04.3 **DONE — pendiente de comprobar exit code del proceso CLI con broker inaccesible.** Generador aplica deadline global configurable por `--timeout-seconds` (600s por defecto), espera ACKs y futures de workers con tiempo restante y aborta transacciones al excederlo (`6db02f8`). Scripts `generate.sh` y `run-scenario.sh` propagan el límite (`5761e29`, `e58f3d2`); README documenta uso (`67cdd9c`). Tests unitarios añadidos para ACK inmediato y future pendiente que vence el deadline (`1b31ea7`, fix de genéricos `719167d9`); CI `44529923824` ejecutó `clean verify` (17 tests, 0 fallos/omitidos), incluidos ACK inmediato y timeout superados. Test unitario adicional usa broker inaccesible (`127.0.0.1:1`), timeout de conexión corto y exige fallo del envío dentro del deadline (`225f3936`); CI `44535458336` superó `clean verify`, incluido test contra `127.0.0.1:1`; falta comprobar exit code del proceso CLI completo.
  - Pasos: mantener contador de futures pendientes; flush al terminar; timeout definido; agregar excepciones; no declarar publicación completa con callbacks pendientes.
  - Artefactos: resultado de generación y logging estructurado.
  - Prueba: simular broker inaccesible y comprobar exit code no cero; caso correcto confirma N ACK.
- [x] B04.4 **DONE.** CI `44535240283` superó `clean verify` tras estabilizar la prueba de particionado con seis particiones y readiness coherente (`1ba53b60`). Tests cubren N=1 y N=10.000 sin huecos/duplicados con 1/2/5/10/20/64 workers y batch 1/500/1000; cinco tamaños de payload; 10.000 ACKs `acks=all`, lectura de los 10.000 y afinidad de clave a partición (`2003e63d`, `cb90566a`, `20b733a5`, `9522ec9d`, `e000779b`, `b375f0b3`, `1ba53b60`).
  - Pasos: testear N=1, N=10k, payloads definidos, threads concurrentes, fallos y particionado por key.
  - Artefactos: tests unitarios e integración.
  - Prueba: ningún ID repetido; particiones reciben claves; payload de longitud solicitada.
**Aceptación:** publicar 10.000 entradas; todas confirmadas y IDs únicos; tiempo de publicación medido aparte.

### B05 — Consumer, transformación y errores
**Depende:** B02, B03, B04.
- [x] B05.1 **DONE.** Listener `@KafkaListener`, manual ack y commit transaccional implementados; test unitario verifica output válido y ACK posterior (`63377dc`); CI `44529574468` `clean verify` pasó y smoke e2e 100 mensajes PASS (`44528276019`).
  - Pasos: suscribir input-topic; group.id fijo; concurrency validada; max.poll.records=BATCH_SIZE; desactivar auto-commit; configurar deserialización segura.
  - Artefactos: KafkaConsumerConfig y listener.
  - Prueba: enviar lote y confirmar recepción; reiniciar y comprobar offsets gestionados por aplicación.
- [x] B05.2 **DONE — pendiente de carga 10k/1M.** Procesamiento genera un output por input válido con messageId, sequence y checksum; test unitario valida output (`63377dc`) y smoke e2e 100 mensajes PASS (`44528276019`).
  - Pasos: validar DTO; medir inicio/fin; invocar workload; construir salida; publicar con key=messageId; esperar ACK antes de permitir commit.
  - Artefactos: MessageProcessor, OutputPublisher y listener.
  - Prueba: N entradas válidas producen N salidas con mismo ID/sequence y checksum esperado.
- [x] B05.3 **DONE — pendiente de prueba de retry/DLT end-to-end con broker.** Error al publicar output propaga excepción y no hace ACK (`8f0dfd47`); backoff exponencial 1s/2s/4s con límite de tres retries (`2a7d2054`, `73ac01bb`). CI `44538095626` superó `clean verify`; DLT transaccional/offset se verifica aparte en B06.4. Falta forzar fallos reales hasta agotar retries y comprobar recuperación DLT en AT_LEAST_ONCE y EXACTLY_ONCE.
  - Pasos: configurar ack manual/record listener; propagar fallo de publish; retry 3 veces con backoff; tras último fallo enviar DLT; confirmar offset solo cuando salida o DLT esté confirmada.
  - Artefactos: error handler/retry config y tests.
  - Prueba: forzar error transitorio y permanente; comprobar número de intentos, DLT y offsets.
- [x] B05.4 **DONE.** Tests cubren JSON malformado, schemaVersion/eventType no soportados, tamaño de payload incorrecto y messageId/sequence inconsistente; verifican DLT antes de ACK y payload/errorType (`63377dc`, `e16621b`). CI `44529923824` ejecutó `clean verify`; los cinco tests de `InputMessageListenerTest` pasaron.
  - Pasos: clasificar errores permanentes vs transitorios; serializar original crudo si JSON inválido; truncar stack/message de error para no inflar DLT; publicar DLT con ACK.
  - Artefactos: DltPublisher y modelo DLT.
  - Prueba: JSON malformado y campo inválido aparecen una sola vez en DLT; no producen output normal.
- [x] B05.5 **DONE — pendiente de prueba de parada durante carga/reinicio.** Spring Boot usa `server.shutdown: graceful` y lifecycle timeout 30s; listener factory aplica Boot defaults y `ContainerProperties.shutdownTimeout=30s` para esperar mensajes en curso (`3e86c59`, `4e3ee55`). CI de smoke ya ejecuta `docker compose down -v` al terminar; la primera CI detectó generic types incompatibles con Boot configurer; corregido a `ConcurrentKafkaListenerContainerFactory<Object,Object>` en `ebf257d5`. CI `44529923824` confirmó build y arranque Spring Boot con la factory personalizada, health UP, smoke Kafka 3 repeticiones PASS y `docker compose down -v` exitoso. Pendiente test que envíe carga durante SIGTERM y verifique offsets/output sin pérdida.
  - Pasos: detener consumo nuevo; terminar/abortar procesamiento en curso según timeout; cerrar producer; dejar offsets sin commit si publicación no confirmó.
  - Artefactos: lifecycle hooks y configuración graceful shutdown.
  - Prueba: matar app durante lote y reiniciar; cada entrada tiene salida validada o sigue pendiente/DLT, sin pérdida.
**Aceptación:** prueba e2e de 10.000 con 0 pérdidas; error inválido en DLT; reinicio sin pérdida silenciosa; duplicados posibles documentados.

### B06 — EXACTLY_ONCE y resiliencia
**Depende:** B05.
- [x] B06.1 **DONE — productor transaccional configurado; pendiente prueba de dos instancias.**
  - Pasos: prefijo transaccional por instancia; producer factory transaccional; aislamiento read_committed en validadores/consumidores; evitar IDs transaccionales duplicados.
  - Artefactos: TransactionConfig y perfiles de entrega.
  - Prueba: dos instancias obtienen transactional IDs distintos y consumidores no ven registros abortados.
- [x] B06.2 **DONE.** Testcontainers verifica envío de output + offset en la misma transacción, abort/retry sin output duplicado visible `read_committed`, DLT + offset transaccional y restart sin replay (`8148554d`); CI `44536265855` superó `clean verify`.
  - Pasos: iniciar transacción; enviar salida; enviar offsets del consumer group a transacción; commit; ante excepción abortar; no hacer commit manual fuera de transacción.
  - Artefactos: TransactionalMessageHandler.
  - Prueba: salida y offset avanzan juntos o ninguno avanza.
- [x] B06.3 **DONE.** CI `81994a9` ejecutó y superó `./mvnw -B clean verify`, incluido el test Testcontainers con Kafka 3.9.1. El test demuestra: abort conjunto de output+offset sin avanzar el offset; `read_committed` no ve el registro abortado; `read_uncommitted` sí lo ve; tras commit, el consumer reiniciado con el mismo groupId no reprocesa la entrada. Evidencia de test: commits `e18851c`, `5d28345`, `81994a9`.
  - Pasos: caso éxito; fallo antes del commit; caída/reinicio; lectura con read_committed y read_uncommitted para demostrar diferencia.
  - Artefactos: tests Testcontainers.
  - Prueba: registros abortados invisibles en read_committed y offsets coherentes.
- [x] B06.4 **DONE.** Testcontainers amplía la prueba EOS: una transacción abortada se reintenta sin duplicar output visible `read_committed`; después procesa una entrada inválida mediante DLT transaccional + commit del offset en la misma transacción, comprueba un único DLT y offset 2, y reinicia el consumer sin replay (`8148554d`); CI `44536265855` superó `clean verify` con el test de abort/retry + DLT transaccional y reinicio.
  - Pasos: inyectar error temporal/permanente; reiniciar consumer; verificar transacción abortada y reintento; comprobar política DLT.
  - Artefactos: pruebas de resiliencia EOS y reporte.
  - Prueba: cada entrada termina en una única salida confirmada o DLT contabilizada.
**Aceptación:** tests reproducibles prueban rollback y atomicidad Kafka; no afirmar EOS en generador ni sistemas externos.

### B07 — Métricas y dashboard
**Depende:** B04, B05.
- [x] B07.1 **DONE.** Tests verifican counters input/output/failure/retry/DLT y timers processing/publish/end-to-end (`e560e8ec`, fix de test registry `d85879df`); CI `44536647815` superó `clean verify`.
  - Pasos: crear counters para entradas/salidas/fallos/retries/DLT; timers para processing/publish/e2e; histogramas con percentiles; nombres estables.
  - Artefactos: MetricsService y configuración Micrometer.
  - Prueba: tests de incrementos/timers y percentiles disponibles.
- [x] B07.2 **DONE.** CI `44534641649` ejecutó smoke PASS de tres repeticiones y obtuvo scrapes reales `/actuator/prometheus`; assertions verificaron `heapUsedBytes`, `gcPauseCount`, `gcPauseSeconds` y `kafkaConsumerLag` no nulos en `environment.json`. `LabMetrics` usa counters/timers sin etiquetas por mensaje/runId, evitando cardinalidad no acotada.
  - Pasos: habilitar health/info/prometheus; restringir endpoints expuestos; revisar tags para cardinalidad; nunca incluir messageId/runId como labels.
  - Artefactos: application.yml y métricas.
  - Prueba: GET /actuator/health UP y /actuator/prometheus contiene series esperadas.
- [x] B07.3 **DONE.** CI `44537286874` y `44537310704` validó provisioning real: Prometheus ready, Grafana health OK, datasource Prometheus (`http://prometheus:9090`) y dashboard UID `kafka-performance-lab` encontrados por API (`98caca27`).
  - Pasos: definir scrape interval; datasource Prometheus; paneles throughput, p50/p95/p99, error, DLT, lag, heap y GC; variables básicas de filtro.
  - Artefactos: YAML provisioning y JSON dashboard.
  - Prueba: dashboard se carga y muestra series tras ejecutar un smoke test.
**Aceptación:** Prometheus scrapea app; métricas aumentan durante e2e; dashboard se carga sin configuración manual.

### B08 — Validador independiente
**Depende:** B04, B05.
- [x] B08.1 **DONE — pendiente de prueba a escala.** Validador espera al commit del consumer group `kafka-lab` antes de fijar endOffsets finales de output/DLT (`2a300e9`); corrige carrera donde podía terminar antes de que el listener publicara todas las salidas. CI `4f92990`: `clean verify` BUILD SUCCESS y smoke e2e de 100 mensajes terminó con `processingComplete=true`, `status=PASS`. Pendiente validar 10k/1M y modos de entrega.
  - Pasos: leer desde beginning para runId; determinar límites/end offsets; recorrer particiones hasta capturar todos los registros del run; excluir transacciones abortadas en EOS.
  - Artefactos: ValidationRunner y Kafka reader independiente.
  - Prueba: validador termina cuando alcanza los offsets objetivo y no depende de contadores internos del consumer.
- [x] B08.2 **DONE — pendiente de perfil de memoria a escala.** Checksums esperados/reales en archivos temporales binarios de 32 bytes por secuencia, eliminados al cerrar; bitsets para presencia y `long[]` primitivo para latencias (~8 bytes/salida), sin strings de checksum ni mensajes retenidos en heap. `DiskChecksumStore` y tests (`4e6e8c1`, `2541e16`, bounds `dbfd513`, `09af720`); integración `97f8a70`, colisión de variable corregida `8d048b3`. CI `8d048b3`: 10 tests, 0 fallos/omitidos, BUILD SUCCESS y smoke 100 mensajes PASS. Falta medir heap/disco con 1M/5M.
  - Pasos: obtener rango esperado 1..N; almacenar IDs vistos en estructura acotada/disco; contar ocurrencias; verificar schema/checksum/sequence; agregar DLT y fallidos.
  - Artefactos: ValidationReport y almacenamiento temporal limpiable.
  - Prueba: dataset sintético detecta missing, duplicate, unexpected, corrupto y DLT por separado.
- [x] B08.3 **DONE.** Testcontainers fixture de B08.4 ejecuta `ValidationRunner` con anomalías conocidas, comprueba que `validation.json` se escribe con contadores exactos y `status=FAIL`, y que `run()` lanza `IllegalStateException` (exit no-cero para el CLI); CI `44533189080` superó `clean verify`.
  - Pasos: incluir expected/valid/failed/DLT/missing/duplicate/unexpected/invalid; incluir runId y timestamp; definir exit 0 solo si criterios PRD pasan.
  - Artefactos: validation.json y CLI validate.
  - Prueba: caso íntegro exit 0; cada anomalía exit no cero.
- [x] B08.4 **DONE.** CI `44533189080` ejecutó 26 tests (0 fallos/errores/omitidos); fixture Testcontainers reportó `processingComplete=true`, `valid=1`, `missing=2`, `duplicate=1`, `unexpected=1`, `invalid=1`, `dlt=1`, `status=FAIL` como se esperaba. CI `44533565949` también superó el test con espera de líderes en los tres topics y ACKs acotados a 10s (`be500325`).
  - Pasos: construir fixtures pequeños controlados; ejecutar validador; comprobar conteos y clasificación exacta.
  - Artefactos: tests unitarios/integración del validador.
  - Prueba: assertions exactas de cada contador.
**Aceptación:** resultados conocidos detectados exactamente; 1M no requiere cargar todos los mensajes en RAM.

### B09 — Orquestación y benchmark
**Depende:** B06, B07, B08.
- [x] B09.1 **DONE.** CI `44527562034` ejecutó `run-scenario.sh` con warm-up 10.000 y tres repeticiones aisladas de 100 mensajes; las tres validaciones reportaron `processingComplete=true`, `status=PASS`. El runner permite reducir warm-up vía `WARMUP_MESSAGES` (default 10.000) para smoke CI (`140b0ed`, `3a51a97`, README `1148002`).
  - Pasos: generar runId nuevo por repetición; evitar borrar topics compartidos/volúmenes; filtrar por runId o recrear topics dedicados de forma controlada; ejecutar warm-up no medido; ejecutar 3 repeticiones secuenciales.
  - Artefactos: BenchmarkRunner y manifiesto de ejecución.
  - Prueba: runIds únicos, warm-up excluido y tres resultados independientes.
- [x] B09.2 **DONE.** Publish timing, end-to-end latency y throughput usan duración entre `publishStartEpochMs` y timestamp de validación; throughput usa outputs únicos validados. Funciones puras extraídas a `benchmark_math.py` y consumidas por `run-scenario.sh` (`ca89fdd8`, `1b2a0fbb`); percentile nearest-rank extraído a `LatencyStatistics` y usado por `ValidationRunner` (`f043f357`, `f6543ef6`).
  - Pasos: capturar nanoTime al inicio/fin de cada etapa; guardar timestamps de mensaje UTC; calcular fórmulas PRD; calcular throughput después de validation PASS; informar percentiles y unidades.
  - Artefactos: BenchmarkMetrics y cálculo estadístico.
  - Prueba: `test_benchmark_math.py` verifica epoch ms→s, throughput, duración cero, negativos y monotonicidad (`11b8d13a`); `LatencyStatisticsTest` verifica p50/p95/p99, límites y entradas inválidas (`63ee2227`).
- [x] B09.3 **DONE.** `run-full-suite.sh --dry-run` emite JSON de matriz: suite principal 1M, concurrencias 1/2/5/10/20, warm-up 10k, 3 repeticiones; payload 1/5/10/50/100 KiB; workloads LIGHT/MEDIUM/HEAVY; delivery AT_LEAST_ONCE/EXACTLY_ONCE; suites secundarias 100k, concurrencia 5. CI `44528732172` ejecutó `run-full-suite.sh --dry-run` y validó todos los valores de la matriz (`4fe77be`, `afdedeef`).
  - Pasos: convertir matriz PRD en escenarios inmutables; aplicar parámetros explícitos; validar argumentos CLI; ejecutar suite principal por defecto y las secundarias solo con --full-suite.
  - Artefactos: ScenarioMatrix, CLI benchmark y scripts.
  - Prueba: dry-run imprime escenarios exactos; tests confirman número/valores de casos.
- [x] B09.4 **DONE — pendiente de probar la matriz secundaria completa.** CI `44528276019`: tres repeticiones, CSV correcto y siete artefactos por run (`offsets.json`, `execution.log`, `validation.json`, `summary.json`, `environment.json`, `report.md`, `prometheus-metrics.txt`) comprobados. Los tres reportes `processingComplete=true`, `status=PASS`. Fixes: permisos host/UID 10001 (`c47c93f`, `dd59680`), readiness (`832afde`), `sed` en vez de `head` por SIGPIPE (`ca1eae7`), métricas a archivo para ARG_MAX (`da34af4`), warm-up configurable y logs (`140b0ed`, `b4fca54`).
  - Pasos: crear directorio por run; escribir archivos atómicamente; usar UTF-8; mantener CSV con columnas fijas; reportar fallos sin borrar logs.
  - Artefactos: seis ficheros obligatorios por ejecución.
  - Prueba: JSON parseable, CSV legible y reporte contiene configuración, tiempos, integridad y estado.
- [x] B09.5 **DONE.** CI `44534641649`: `clean verify` (27 tests, 0 fallos/errores/omitidos) y smoke de tres repeticiones PASS; cada repetición generó `resource-samples.tsv`, `resource-samples.json` y scrapes Prometheus periódicos. CI exigió `memoryBytes`, `memoryPercentMax`, `heapUsedBytes`, `gcPauseCount`, `gcPauseSeconds`, `kafkaConsumerLag` no nulos y consistencia de `resourceSampleCount`. Muestreo durante generación/validación configurable a 5s (`fd6b0b76`), máximos/lag/deltas GC; README `e6cb359c`.
  - Pasos: consultar métricas JVM/Prometheus y Docker stats; muestrear durante ejecución; registrar promedio/máximo; si no hay permiso, guardar unavailable + motivo.
  - Artefactos: environment.json y summary.json.
  - Prueba: campos presentes con valor o motivo explícito; no usar cero para dato desconocido.
- [x] B09.6 **DONE — pendiente de ejecución real en hardware objetivo.** CI `44534641649` validó dry-run incremental (5M max, pasos 100k, una repetición, warm-up 10k) y los tests de política; CI `44534778517` volvió a superar `py_compile`, tests unitarios y dry-run antes de fallar en la integración Kafka B04.4 no relacionada. `stress_runner.py` guarda cada escalón en `stress-history.jsonl`, ejecuta validador y detiene ante errores, memoria >85%, lag creciente 3 escalones o métricas ausentes (`eab1766c`, `90d3ed26`, `c0a034f0`, `be934a66`, `fa28c2a8`, `2ea90af9`).
  - Pasos: aumentar en 100k; validar cada paso; mantener histórico; detener ante error >0, lag creciente 3 pasos o memoria >85%, o al llegar a 5M.
  - Artefactos: stress runner y resultados por escalón.
  - Prueba: tests unitarios de cada condición de parada.
- [x] B09.7 **DONE.** Aislamiento lógico mediante runId único y snapshots de offsets; validador procesa solo el rango de cada ejecución y espera el commit del consumer group antes de cerrar. Tres repeticiones secuenciales pasaron en CI `44528276019`; el stack CI se limpia con `docker compose down -v`. PRD actualizado para documentar snapshots en vez de truncar topics (`a6122c6`).
  - Pasos: aislar por runId; limitar borrado a topics de laboratorio creados por script; preservar benchmark-results; borrar solo temporales propios al cerrar.
  - Artefactos: RunIsolation/Cleanup.
  - Prueba: ejecutar dos runs y demostrar que el segundo no mezcla ni elimina el primero.
**Aceptación:** benchmark 1M ejecuta o deja blocker inequívoco; cifras reproducibles; cada run contiene todos los artefactos; datos validados antes de calcular throughput.

### B10 — Experiencia Windows/Linux, documentación y cierre
**Depende:** B02, B09.
- [x] B10.1 **DONE — pendiente de prueba Windows.** Pares `.bat`/`.sh` creados; launchers Windows ahora preparan permisos de escritura del directorio `benchmark-results` mediante ACL antes de arrancar Compose; añadido `scripts\\run-stress.bat` (`3e3050e`) (`8f2a476`, `05df37f`, `6f2e8a3`, `bb40713`). Pendiente ejecutar en Windows real.
  - Pasos: crear pares .bat/.sh; validar argumentos; cambiar al directorio raíz; propagar exit codes; imprimir ayuda breve; scripts idempotentes.
  - Artefactos: scripts/ y launchers raíz.
  - Prueba: invocar ayuda, start/stop y benchmark smoke desde Windows y shell cuando disponibles.
- [x] B10.2 **DONE — pendiente de pruebas en Windows real.** Acceptance alineada con launcher autónomo: detecta/inicia Docker Desktop, espera máximo 120s, exige Compose v2, ejecuta stack/benchmark directamente y preserva exit code; no invoca Claude ni usa bypasses de permisos. ACL de `benchmark-results` limitada a usuario actual y llamadas Git Bash entre comillas para rutas con espacios (`58935f9a`, `134e45a1`, `4a34f3f8`, `c145faaf`, `2ebaa882`, `4d2fb984`, `2b560d7f`). AGENTS fija política (`403f363a`); tests estáticos cubren rutas, ACL y seguridad (`8a939247`); CI `44537956676` superó `clean verify` con estos tests. Pendiente probar Docker Desktop activo/detenido/no instalado y rutas con espacios en Windows.
  - Pasos: comprobar comandos; localizar Docker Desktop; iniciar si detenido; esperar máximo 120s; verificar PRD/BACKLOG; ejecutar Claude con instrucciones mínimas; preservar exit code y pausar solo al final.
  - Artefactos: RUN-KAFKA-LAB.bat.
  - Prueba: revisar rutas con espacios, Docker detenido/activo/no instalado y Claude ausente; no ejecutar permisos peligrosos sin advertencia visible.
- [x] B10.3 **DONE — pendiente validación desde clon limpio.** README cubre instalación, quick start, comandos Windows/Linux, variables Compose/Spring, parámetros de runner/benchmark/stress, resultados, puertos, métricas y troubleshooting (`c0666d0`, `5ee379cb`). Pendiente revisar desde un clon limpio en Windows y Linux.
  - Pasos: describir instalación exacta; quick start; parada; suites; endpoints; parámetros; problemas comunes; ruta de resultados.
  - Artefactos: README.md.
  - Prueba: persona con clon limpio puede seguir pasos sin consultar conversación.
- [x] B10.4 **DONE — pendiente benchmark 1M/matriz completa en hardware objetivo.** CI `44529923824`: `clean verify` BUILD SUCCESS (17 tests, 0 fallos, 0 omitidos; incluye Testcontainers Kafka EOS y tests de timeout/DLT), Docker image build SUCCESS con Maven cache/retries, health UP, smoke de warm-up 100 y tres repeticiones de 100 mensajes (`processingComplete=true`, `status=PASS`), siete artefactos y cinco métricas runtime no nulas; `docker compose down -v` SUCCESS. Dry-run matriz PRD también validado.
  - Pasos: ejecutar build; unit/integration; compose; smoke pequeño; 1M; validador; suites extra solo si solicitadas; guardar comandos y exit codes.
  - Artefactos: informes y logs de ejecución.
  - Prueba: evidencias verificables; si el entorno bloquea, estado BLOCKED con causa concreta.
- [ ] B10.5 **BLOCKED — revisión final y evidencias de entorno pendientes.**
  - Pasos: inspeccionar diff/status; buscar secretos; comprobar .gitignore; verificar scripts; resumir baseline/optimizado y limitaciones; commit final.
  - Artefactos: cambios fuente limpios e informe agregado.
  - Prueba: git diff --check; git status sin temporales; comandos documentados reproducibles.
### B10.6 — Estándar SOLID obligatorio
- [x] B10.6 **DONE — estándar documentado y aplicado a las instrucciones del agente.** Añadido `docs/solid-principles.md` como guía del proyecto y referenciado desde `AGENTS.md`. Se mantiene la arquitectura Spring Boot por capas; no se introduce migración hexagonal ni interfaces mecánicas. Evidencia: commits `5171d12` y `655401b`.
  - Pasos: incorporar los cinco principios; definir límites Spring Boot; añadir checklist y precedencia documental; fijar revisión SOLID para código nuevo/modificado.
  - Artefactos: `docs/solid-principles.md`, `AGENTS.md`.
  - Prueba: revisión de consistencia con PRD/AGENTS y confirmación de que la guía no exige nuevas dependencias ni cambios funcionales.

### B10.8 — Convenciones obligatorias de anotaciones Lombok y Spring
- [x] B10.8 **DONE — pendiente de verificación CI.** Convenciones documentadas en `AGENTS.md` y auditadas sobre el código Java de `main`. Ajustados cinco beans genéricos de `@Service` a `@Component` (`LabMetrics`, `ChecksumService`, `InputMessageProcessor`, `WorkloadProcessor`, `OutputPublisher`). Se conserva constructor injection explícita: el proyecto no declara Lombok y no se añade la dependencia solo para sustituir constructores correctos; no se detectó necesidad de `@Slf4j` ni builders. No se introdujo `@Autowired`. Evidencia: `1687d3b`; pendiente `./mvnw -B clean verify` en CI.
  - Pasos: incorporar las convenciones a `AGENTS.md`; revisar solapamientos con constructor injection/SOLID; registrar fuente externa.
  - Artefactos: `AGENTS.md`.
  - Prueba: revisión documental de consistencia; CI no aplica porque no cambia código ejecutable.
  - Fuente: https://github.com/juanfranciscofernandezherreros/springboot-agent-rules/blob/master/docs/annotations.md

### B10.9 — Excepciones propias y semántica de errores
- [ ] B10.9 **BLOCKED — aplicación parcial; auditoría completa y CI pendientes.** Regla añadida a AGENTS.md (9ee7b51). Excepciones propias incorporadas para entrada inválida, publicación Kafka, serialización, fallo de validación, checksum y generación. Los tests cubren DLT, publicación sin ACK, causa y fallo de validación; se añadieron los wrappers para worker/snapshot y se conserva la interrupción. Ver commits de B10.9.
  - [x] B10.9.1 **DONE — pendiente de verificación CI.** Añadida `InvalidInputMessageException` y aplicada al processor/listener; los tests esperan el tipo propio en DLT.
  - [x] B10.9.2 **DONE — pendiente de verificación CI.** Clasificados fallos de publicación, serialización, recuperación transaccional, validación final, checksum y generación con tipos propios; se conserva la causa y se restaura el flag de interrupción al interrumpir esperas Kafka. Las precondiciones técnicas locales mantienen excepciones estándar permitidas por AGENTS.md.
  - [ ] B10.9.3 **BLOCKED — pendiente de barrido completo y verificación automatizada.** Reemplazadas excepciones de aplicación en consumer, producer, generator, validator y message ID; catches de validación se acotaron para propagar fallos I/O; se envuelven también fallos Kafka síncronos. Los catches genéricos restantes están en límites de recuperación/snapshot o limpieza; falta completar barrido de scripts/tests y ejecutar CI.
  - [x] B10.9.3.1 **DONE — pendiente de verificación CI.** Añadidas `InvalidCommandLineException` para opciones CLI inválidas y `InvalidMessageIdException` para IDs que incumplen el contrato; adaptados generador, validador y test de `MessageIdFactory`. Commits: `4d4cc8f`, `c97990e`, `9eaa2cc`, `39b18ff`, `6151b11`, `6c0c2ac`.
  - [x] B10.9.3.2 **DONE — pendiente de verificación CI.** Aplicada `InvalidCommandLineException` a valores CLI ausentes, vacíos, no numéricos o no positivos en generador y validador; se valida también `run-id`. Commits: `4d4cc8f`, `6151b11`, `6c0c2ac`, `1197033`, `eaf0479`. Se acotó además la clasificación de registros inválidos en `ValidationRunner` para propagar fallos I/O del almacenamiento de checksums (commit `3fbe9c9`).
  - Prueba: `./mvnw -B clean verify`, pruebas focalizadas de DLT/reintentos/publicación/validación y smoke de transacciones EOS. Estado CI del commit `f11b172`: sin checks reportados; no se declara PASS.

### B10.7 — Auditoría y refactor SOLID del código existente
- [ ] B10.7 **BLOCKED — refactor parcial; quedan responsabilidades por separar y CI pendiente.** Primera inspección: `InputMessageListener` combinaba deserialización, validación de entrada, política de procesamiento, publicación Kafka, gestión de offsets y métricas; `GeneratorRunner` combina CLI, concurrencia, serialización, publicación y snapshots de offsets; `ValidationRunner` combina lectura Kafka, validación de contrato/integridad, estadísticas y escritura de informes. No crear interfaces hasta identificar contratos reales.
  - [x] B10.7.1 **DONE — pendiente de verificación CI.** Extraída la política de validación y transformación a `InputMessageProcessor`; el listener queda como adaptador de deserialización, transporte Kafka, ACK/offset y métricas. Tests del listener adaptados para conservar casos de JSON inválido, DLT, salida válida y no-ACK ante fallo de publicación. Commits: `038e5ea`, `ef1caff`, `134eb35`, `256ac17`. No se ha ejecutado Maven localmente.
  - [ ] B10.7.2 **BLOCKED — pendiente.** Separar responsabilidades de generación solo donde tengan ciclo de cambio independiente, manteniendo orden, timeout global, paralelismo, snapshot de offsets y claves deterministas.
  - [x] B10.7.2.1 **DONE — pendiente de verificación CI.** Extraída la captura de offsets iniciales a `GeneratorOffsetSnapshot`; `GeneratorRunner` delega el snapshot manteniendo runId, topics y offsets. Commits: `17de6da`, `b4d61e1`.
  - [ ] B10.7.3 **BLOCKED — pendiente.** Separar lectura Kafka, validación de contrato/integridad, estadísticas y escritura de informes sin aumentar memoria ni cambiar aislamiento por runId.
  - [x] B10.7.3.1 **DONE — pendiente de verificación CI.** Extraída la escritura JSON/consola a `ValidationReportWriter`; `ValidationRunner` mantiene la lectura, clasificación, integridad y política de aceptación. Commits: `e2ce38c`, `86c2cea`, `6555bb0`.
  - [ ] B10.7.4 **BLOCKED — pendiente.** Revisar DIP/ISP/OCP y contratos de implementaciones donde haya variaciones reales; añadir pruebas de contrato compartidas cuando existan implementaciones intercambiables.
  - Artefactos: clases Java afectadas y tests JUnit/Testcontainers existentes o ampliados; actualizar README/PRD solo si cambia comportamiento (no previsto).
  - [x] B10.7.5 **DONE — pendiente de verificación CI.** Aplicados nombres descriptivos a condiciones compuestas y reglas de aceptación en Java; añadidos nombres explícitos a condiciones de política de estrés y validación matemática Python y configuración transaccional. Commits adicionales: fd831a4, add5b8a, 589136d, 8ed5e02, 01af252, ffe47ee, fdd8c8d, 54a548e. Pendiente ./mvnw -B clean verify, py_compile y tests Python.
  - Prueba: `./mvnw -B clean verify`; pruebas de contrato para offsets/ACK, DLT y transacciones EOS; smoke CI del flujo; comparar artefactos y métricas antes/después. No aceptar regresiones ni afirmar mejora de rendimiento sin medición.

**Aceptación final:** PRD Definition of Done satisfecho; instrucciones funcionan desde clon limpio; informe final distingue ejecutado, fallido y bloqueado.

## Grafo resumido
B00 -> B01 -> B02 -> B04 -> B05 -> B06 -> B09 -> B10
B01 -> B03 -> B04
B04+B05 -> B07
B04+B05 -> B08
B06+B07+B08 -> B09

## Protocolo de commits por micro-paso
Para cada acción independiente dentro de “Pasos”:
1. Implementar solo esa acción.
2. Ejecutar comprobación focalizada; si procede, ejecutar `./mvnw -q -DskipTests=false test` o el test específico.
3. Revisar `git diff --check` y `git status --short`.
4. Crear un commit real con formato `tipo(ID): verbo + objeto`; tipos permitidos: feat, fix, test, build, docs, refactor, chore.
5. No incluir cambios ajenos ni resultados masivos. No crear commits vacíos para comandos de inspección.
6. Continuar con el siguiente micro-paso.
Al terminar el ID padre, ejecutar su prueba de aceptación completa y actualizar TODO -> DONE, DONE — pendiente de verificación o BLOCKED con evidencia. Esa actualización también debe quedar en un commit visible.

Ejemplos:
- `build(B01.1): add Spring Boot dependency management`
- `feat(B01.1): configure Kafka and actuator starters`
- `test(B03.2): verify deterministic message IDs`
- `fix(B05.3): acknowledge offsets after output confirmation`

## Protocolo de cierre por tarea
En cada checkbox sustituir TODO por DONE solo tras pasar aceptación; si no, BLOCKED con causa y evidencia. Registrar en una línea: `ID | estado | comando/prueba | resultado`. No pegar logs extensos en este archivo: guardar logs bajo benchmark-results. Si cambia alcance/arquitectura, actualizar primero PRD y este backlog en el mismo commit.

## Criterio global
No existe DONE global por tener código escrito. Solo al cumplir B10.5 con evidencias reales. Si el entorno impide ejecutar una prueba, marcarla BLOCKED, explicar el requisito externo y completar el resto.
