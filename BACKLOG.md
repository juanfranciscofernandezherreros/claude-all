# BACKLOG — Kafka Performance Lab

Este backlog es el plan obligatorio de implementación. Ejecutar en orden; las dependencias son bloqueantes. PRD.md define el contrato funcional y prevalece ante detalles de negocio. No añadir tecnologías ni alcance. Cada tarea se cierra solo con sus evidencias; actualizar su estado en este archivo. Estados iniciales: TODO.

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
- [ ] B00.1 **BLOCKED — inspección parcial remota.** GitHub confirma repositorio `claude-all`, rama `main`, permisos de lectura/escritura y seis archivos de documentación/launcher; no existe código fuente ni árbol Maven todavía. No hay acceso a la máquina local desde la sesión GitHub, por lo que Java, Docker Desktop, SO, CPU y RAM no se pueden verificar aquí. Reanudar esta subtarea desde el equipo local antes de ejecutar Docker/benchmarks.
  - Pasos: comprobar rama/estado Git; listar archivos; comprobar java -version, docker version/info, docker compose version, git --version y claude --version; identificar SO/arquitectura/CPU/RAM; registrar faltantes.
  - Artefactos: inventario breve en BACKLOG (evidencia), sin volcar logs.
  - Prueba: inventario remoto completado mediante GitHub API; comprobación local de Java/Docker/CPU/RAM pendiente. No marcar DONE hasta completar la inspección local.
- [ ] B00.2 Verificar coherencia PRD: messageId string `runId-sequence`; tiempos UTC en mensajes y monotónicos en medición; diferencia entre procesamiento y end-to-end.
  - Pasos: contrastar esquemas entrada/salida/DLT; comprobar reglas EOS/at-least-once; confirmar fórmulas, escenarios y variables; corregir contradicciones en PRD antes del código.
  - Artefactos: PRD corregido solo si existe contradicción real.
  - Prueba: revisar que backlog y PRD usan mismos nombres, valores y criterios.
- [ ] B00.3 Crear árbol Maven indicado y .gitignore para target, logs, volúmenes y resultados masivos; conservar solo informes agregados pequeños.
  - Pasos: crear directorios del layout; ignorar target, .env, logs, datos Kafka, temporales y resultados de gran tamaño; permitir .env.example y resumen de benchmark.
  - Artefactos: árbol base, .gitignore.
  - Prueba: git check-ignore confirma artefactos locales y git status conserva los archivos fuente.
**Aceptación:** inventario y árbol creados; no hay decisiones abiertas ni contradicciones en contrato.

### B01 — Proyecto y configuración
**Depende:** B00.
- [ ] B01.1 Crear pom.xml con Java 21, Spring Boot 3.4.5, Spring Kafka, Actuator/Micrometer, Prometheus registry, Jackson, JUnit 5 y Testcontainers; fijar versiones compatibles.
  - Pasos: usar parent/BOM Spring Boot; incluir starter web, kafka, actuator, validation, micrometer-registry-prometheus, Jackson, test starter y Testcontainers Kafka; fijar plugin compiler/surefire; evitar dependencias redundantes.
  - Artefactos: pom.xml.
  - Prueba: mvn dependency:tree sin conflictos; compiler release 21.
- [ ] B01.2 Añadir Maven Wrapper 3.9.9 y configuración reproducible; compilar app mínima.
  - Pasos: generar wrapper oficial; comprobar scripts Unix/Windows y wrapper jar/properties; crear clase main Spring Boot y contexto mínimo.
  - Artefactos: mvnw, mvnw.cmd, .mvn/wrapper/*, clase de arranque.
  - Prueba: ./mvnw -version, mvnw.cmd -version y clean package.
- [ ] B01.3 Crear application.yml y binding tipado de variables del PRD; defaults, validación de rangos y fallo temprano con mensaje claro.
  - Pasos: agrupar propiedades kafka/app/benchmark/metrics; mapear cada variable PRD; validar enum/rango/positividad; no registrar secretos; documentar defaults.
  - Artefactos: application.yml, clases @ConfigurationProperties y tests.
  - Prueba: contexto arranca con defaults; valores inválidos impiden arrancar con mensaje accionable.
- [ ] B01.4 Crear .env.example sin secretos y documentación de todas las variables.
  - Pasos: listar variable, default, rango, unidad y efecto; mantener Compose y Spring con nombres idénticos; no incluir credenciales reales.
  - Artefactos: .env.example y tabla de variables en README.
  - Prueba: Compose interpola el ejemplo y app arranca con configuración por defecto.
**Aceptación:** `./mvnw clean verify` y `mvnw.cmd clean verify` funcionan; configuración inválida falla con código distinto de cero.

### B02 — Kafka local reproducible
**Depende:** B01.
- [ ] B02.1 Crear Dockerfile multi-stage reproducible para app.
  - Pasos: etapa build con JDK 21 y Maven Wrapper; etapa runtime JRE 21 no-root; copiar solo artefacto; healthcheck app si procede; fijar imagen base.
  - Artefactos: Dockerfile y .dockerignore.
  - Prueba: docker build finaliza y contenedor arranca con usuario no-root.
- [ ] B02.2 Crear compose.yaml con kafka KRaft, app, Prometheus y Grafana; versiones fijas, listeners interno/externo, volumen persistente y puertos PRD.
  - Pasos: configurar node.id/roles/controller quorum/listeners/advertised listeners; definir red, volumen, restart policy, límites razonables y puertos; nunca latest.
  - Artefactos: compose.yaml y configuración Kafka.
  - Prueba: docker compose config válida; cliente desde host y contenedor resuelve listeners correctos.
- [ ] B02.3 Configurar healthcheck Kafka verificando broker realmente disponible; dependencias condicionadas a healthy.
  - Pasos: usar comando kafka-broker-api-versions o equivalente incluido en imagen; definir interval/timeout/retries/start_period; app depende de service_healthy.
  - Artefactos: healthcheck y depend_on en compose.
  - Prueba: arranque frío pasa de starting a healthy; detener broker provoca unhealthy.
- [ ] B02.4 Crear topics input/output/DLT con 24 particiones y RF=1 idempotentemente al iniciar.
  - Pasos: script/init container espera broker; crea topics con nombres exactos; si ya existen, valida configuración y no falla; definir retención para suite.
  - Artefactos: init script/servicio y definición de topics.
  - Prueba: dos arranques seguidos sin error; describe topics confirma particiones/replication factor.
- [ ] B02.5 Provisionar Prometheus y Grafana automáticamente, sin pasos manuales.
  - Pasos: crear prometheus.yml scrape app; datasource provisioning; dashboard provisioning; credenciales locales no sensibles y documentadas.
  - Artefactos: monitoring/prometheus/* y monitoring/grafana/provisioning/*.
  - Prueba: targets Prometheus UP; dashboard visible tras arrancar Compose.
**Aceptación:** `docker compose up -d --build`; todos healthy; host conecta a localhost:9094; contenedores conectan a kafka:9092; topics exactos presentes; reinicio conserva volumen.

### B03 — Contrato de dominio
**Depende:** B01.
- [ ] B03.1 Crear DTOs versionados de entrada, salida y DLT; validaciones obligatorias y timestamps UTC.
  - Pasos: declarar campos/tipos exactos del PRD; Bean Validation para null/vacíos/rangos; UTC ISO-8601; schemaVersion=1; preservar entrada original en DLT.
  - Artefactos: model/InputMessage, OutputMessage, DltMessage y tests.
  - Prueba: serialización JSON golden tests y rechazo de cada campo obligatorio inválido.
- [ ] B03.2 Crear fábrica de IDs `runId-sequence` y checksum SHA-256 UTF-8 determinista.
  - Pasos: generar UUID por run; construir ID con secuencia decimal estable; checksum bytes UTF-8 en hexadecimal; evitar dependencia del reloj.
  - Artefactos: RunIdProvider, MessageIdFactory, ChecksumService.
  - Prueba: vectores SHA-256 conocidos; misma entrada produce mismo ID/checksum; secuencias no colisionan.
- [ ] B03.3 Implementar LIGHT/MEDIUM/HEAVY con 1/10/100 rondas SHA-256 sin sleep.
  - Pasos: parsear enum; aplicar exactamente N rondas sobre bytes de resultado anterior; evitar optimización que elimine trabajo; no introducir pausas.
  - Artefactos: WorkloadProcessor y tests.
  - Prueba: outputs deterministas y coste creciente comprobable sin exigir tiempos absolutos.
- [ ] B03.4 Añadir tests de serialización, validación, IDs, checksum y tamaños de payload.
  - Pasos: cubrir casos válidos, campos ausentes, JSON corrupto, Unicode, payload mínimo/máximo y límites configurados.
  - Artefactos: tests unitarios JUnit.
  - Prueba: mvnw -Dtest=*Model*,*Checksum*,*MessageId* test (ajustar patrones a nombres reales).
**Aceptación:** tests cubren límites, JSON inválido, determinismo e igualdad de ID/sequence.

### B04 — Productor y generador
**Depende:** B02, B03.
- [ ] B04.1 Configurar producer con acks=all, idempotencia, retries y batch/linger/compression configurables.
  - Pasos: definir bootstrap según entorno; key=messageId; serializer JSON; acks all; enable.idempotence true; retries compatibles; batch.size/linger/compression externos.
  - Artefactos: KafkaProducerConfig y propiedades.
  - Prueba: inspeccionar configuración efectiva y publicar/leer mensaje de prueba.
- [ ] B04.2 Implementar generador CLI que publique N mensajes únicos; threads configurables, payload exacto, progreso cada 10.000, runId y métricas de publicación.
  - Pasos: validar argumentos; crear runId; generar secuencias 1..N sin duplicar; construir payload con bytes exactos; distribuir envío entre threads; usar callbacks/acks.
  - Artefactos: generator CLI, payload factory y documentación de flags.
  - Prueba: generar 10.000; contar IDs distintos y comprobar payload bytes y secuencia continua.
- [ ] B04.3 Esperar confirmación de todos los envíos; fallo de envío produce exit code no cero y registro.
  - Pasos: mantener contador de futures pendientes; flush al terminar; timeout definido; agregar excepciones; no declarar publicación completa con callbacks pendientes.
  - Artefactos: resultado de generación y logging estructurado.
  - Prueba: simular broker inaccesible y comprobar exit code no cero; caso correcto confirma N ACK.
- [ ] B04.4 Tests de generación, distribución por partición, tamaño, unicidad y error de publicación.
  - Pasos: testear N=1, N=10k, payloads definidos, threads concurrentes, fallos y particionado por key.
  - Artefactos: tests unitarios e integración.
  - Prueba: ningún ID repetido; particiones reciben claves; payload de longitud solicitada.
**Aceptación:** publicar 10.000 entradas; todas confirmadas y IDs únicos; tiempo de publicación medido aparte.

### B05 — Consumer, transformación y errores
**Depende:** B02, B03, B04.
- [ ] B05.1 Implementar consumer con group kafka-lab, concurrencia y batch/poll configurables.
  - Pasos: suscribir input-topic; group.id fijo; concurrency validada; max.poll.records ligado a batch; desactivar auto-commit; configurar deserialización segura.
  - Artefactos: KafkaConsumerConfig y listener.
  - Prueba: enviar lote y confirmar recepción; reiniciar y comprobar offsets gestionados por aplicación.
- [ ] B05.2 Procesar, transformar y publicar una salida por entrada válida; preservar messageId y sequence.
  - Pasos: validar DTO; medir inicio/fin; invocar workload; construir salida; publicar con key=messageId; esperar ACK antes de permitir commit.
  - Artefactos: MessageProcessor, OutputPublisher y listener.
  - Prueba: N entradas válidas producen N salidas con mismo ID/sequence y checksum esperado.
- [ ] B05.3 AT_LEAST_ONCE: confirmar offset solo tras ACK de salida; reintentos 3 y backoff 1/2/4s (cap 10s); después DLT.
  - Pasos: configurar ack manual/record listener; propagar fallo de publish; retry 3 veces con backoff; tras último fallo enviar DLT; confirmar offset solo cuando salida o DLT esté confirmada.
  - Artefactos: error handler/retry config y tests.
  - Prueba: forzar error transitorio y permanente; comprobar número de intentos, DLT y offsets.
- [ ] B05.4 Errores de validación directos a DLT; DLT incluye entrada original, tipo/mensaje de error y timestamp.
  - Pasos: clasificar errores permanentes vs transitorios; serializar original crudo si JSON inválido; truncar stack/message de error para no inflar DLT; publicar DLT con ACK.
  - Artefactos: DltPublisher y modelo DLT.
  - Prueba: JSON malformado y campo inválido aparecen una sola vez en DLT; no producen output normal.
- [ ] B05.5 Manejo de shutdown y excepciones para no confirmar prematuramente.
  - Pasos: detener consumo nuevo; terminar/abortar procesamiento en curso según timeout; cerrar producer; dejar offsets sin commit si publicación no confirmó.
  - Artefactos: lifecycle hooks y configuración graceful shutdown.
  - Prueba: matar app durante lote y reiniciar; cada entrada tiene salida validada o sigue pendiente/DLT, sin pérdida.
**Aceptación:** prueba e2e de 10.000 con 0 pérdidas; error inválido en DLT; reinicio sin pérdida silenciosa; duplicados posibles documentados.

### B06 — EXACTLY_ONCE y resiliencia
**Depende:** B05.
- [ ] B06.1 Configurar transacciones Kafka, transactional.id único por instancia y read_committed.
  - Pasos: prefijo transaccional por instancia; producer factory transaccional; aislamiento read_committed en validadores/consumidores; evitar IDs transaccionales duplicados.
  - Artefactos: TransactionConfig y perfiles de entrega.
  - Prueba: dos instancias obtienen transactional IDs distintos y consumidores no ven registros abortados.
- [ ] B06.2 Publicar salida y confirmar offsets en una transacción atómica.
  - Pasos: iniciar transacción; enviar salida; enviar offsets del consumer group a transacción; commit; ante excepción abortar; no hacer commit manual fuera de transacción.
  - Artefactos: TransactionalMessageHandler.
  - Prueba: salida y offset avanzan juntos o ninguno avanza.
- [ ] B06.3 Añadir pruebas de commit, abort/rollback, reinicio durante transacción y aislamiento read_committed.
  - Pasos: caso éxito; fallo antes del commit; caída/reinicio; lectura con read_committed y read_uncommitted para demostrar diferencia.
  - Artefactos: tests Testcontainers.
  - Prueba: registros abortados invisibles en read_committed y offsets coherentes.
- [ ] B06.4 Verificar recuperación/retry/DLT sin duplicación lógica en modo EOS.
  - Pasos: inyectar error temporal/permanente; reiniciar consumer; verificar transacción abortada y reintento; comprobar política DLT.
  - Artefactos: pruebas de resiliencia EOS y reporte.
  - Prueba: cada entrada termina en una única salida confirmada o DLT contabilizada.
**Aceptación:** tests reproducibles prueban rollback y atomicidad Kafka; no afirmar EOS en generador ni sistemas externos.

### B07 — Métricas y dashboard
**Depende:** B04, B05.
- [ ] B07.1 Registrar contadores/timers del PRD y percentiles p50/p95/p99.
  - Pasos: crear counters para entradas/salidas/fallos/retries/DLT; timers para processing/publish/e2e; histogramas con percentiles; nombres estables.
  - Artefactos: MetricsService y configuración Micrometer.
  - Prueba: tests de incrementos/timers y percentiles disponibles.
- [ ] B07.2 Exponer Actuator/Prometheus; excluir IDs de labels.
  - Pasos: habilitar health/info/prometheus; restringir endpoints expuestos; revisar tags para cardinalidad; nunca incluir messageId/runId como labels.
  - Artefactos: application.yml y métricas.
  - Prueba: GET /actuator/health UP y /actuator/prometheus contiene series esperadas.
- [ ] B07.3 Configurar scrape de app y dashboard Grafana provisionado con throughput, latencia, errores, DLT, lag y JVM.
  - Pasos: definir scrape interval; datasource Prometheus; paneles throughput, p50/p95/p99, error, DLT, lag, heap y GC; variables básicas de filtro.
  - Artefactos: YAML provisioning y JSON dashboard.
  - Prueba: dashboard se carga y muestra series tras ejecutar un smoke test.
**Aceptación:** Prometheus scrapea app; métricas aumentan durante e2e; dashboard se carga sin configuración manual.

### B08 — Validador independiente
**Depende:** B04, B05.
- [ ] B08.1 Crear validador separado del consumer que recorra output-topic con read_committed cuando aplique.
  - Pasos: leer desde beginning para runId; determinar límites/end offsets; recorrer particiones hasta capturar todos los registros del run; excluir transacciones abortadas en EOS.
  - Artefactos: ValidationRunner y Kafka reader independiente.
  - Prueba: validador termina cuando alcanza los offsets objetivo y no depende de contadores internos del consumer.
- [ ] B08.2 Comparar set esperado de IDs vs outputs con memoria acotada (archivo/DB temporal o algoritmo streaming); detectar missing, duplicate, unexpected, invalid y DLT.
  - Pasos: obtener rango esperado 1..N; almacenar IDs vistos en estructura acotada/disco; contar ocurrencias; verificar schema/checksum/sequence; agregar DLT y fallidos.
  - Artefactos: ValidationReport y almacenamiento temporal limpiable.
  - Prueba: dataset sintético detecta missing, duplicate, unexpected, corrupto y DLT por separado.
- [ ] B08.3 Emitir validation.json y código de salida no cero si integridad incumple criterios.
  - Pasos: incluir expected/valid/failed/DLT/missing/duplicate/unexpected/invalid; incluir runId y timestamp; definir exit 0 solo si criterios PRD pasan.
  - Artefactos: validation.json y CLI validate.
  - Prueba: caso íntegro exit 0; cada anomalía exit no cero.
- [ ] B08.4 Añadir tests con datasets de pérdida, duplicado, extra y corrupto.
  - Pasos: construir fixtures pequeños controlados; ejecutar validador; comprobar conteos y clasificación exacta.
  - Artefactos: tests unitarios/integración del validador.
  - Prueba: assertions exactas de cada contador.
**Aceptación:** resultados conocidos detectados exactamente; 1M no requiere cargar todos los mensajes en RAM.

### B09 — Orquestación y benchmark
**Depende:** B06, B07, B08.
- [ ] B09.1 Implementar runner por runId: limpiar estado de topics/offsets de forma segura, warm-up 10k y 3 repeticiones medidas.
  - Pasos: generar runId nuevo por repetición; evitar borrar topics compartidos/volúmenes; filtrar por runId o recrear topics dedicados de forma controlada; ejecutar warm-up no medido; ejecutar 3 repeticiones secuenciales.
  - Artefactos: BenchmarkRunner y manifiesto de ejecución.
  - Prueba: runIds únicos, warm-up excluido y tres resultados independientes.
- [ ] B09.2 Implementar medición monotónica publish/processing/end-to-end; throughput solo outputs únicos validados; latencias min/mean/p50/p95/p99/max.
  - Pasos: capturar nanoTime al inicio/fin de cada etapa; guardar timestamps de mensaje UTC; calcular fórmulas PRD; calcular throughput después de validation PASS; informar percentiles y unidades.
  - Artefactos: BenchmarkMetrics y cálculo estadístico.
  - Prueba: tests con reloj/datos controlados verifican fórmulas y unidades.
- [ ] B09.3 Implementar suite principal 1M por concurrency 1/2/5/10/20 y suites --full-suite de payload/workload/delivery según PRD.
  - Pasos: convertir matriz PRD en escenarios inmutables; aplicar parámetros explícitos; validar argumentos CLI; ejecutar suite principal por defecto y las secundarias solo con --full-suite.
  - Artefactos: ScenarioMatrix, CLI benchmark y scripts.
  - Prueba: dry-run imprime escenarios exactos; tests confirman número/valores de casos.
- [ ] B09.4 Guardar summary.json, results.csv, validation.json, environment.json, execution.log y report.md por run.
  - Pasos: crear directorio por run; escribir archivos atómicamente; usar UTF-8; mantener CSV con columnas fijas; reportar fallos sin borrar logs.
  - Artefactos: seis ficheros obligatorios por ejecución.
  - Prueba: JSON parseable, CSV legible y reporte contiene configuración, tiempos, integridad y estado.
- [ ] B09.5 Capturar CPU/memoria/heap/GC/lag cuando disponibles; anotar ausencias.
  - Pasos: consultar métricas JVM/Prometheus y Docker stats; muestrear durante ejecución; registrar promedio/máximo; si no hay permiso, guardar unavailable + motivo.
  - Artefactos: environment.json y summary.json.
  - Prueba: campos presentes con valor o motivo explícito; no usar cero para dato desconocido.
- [ ] B09.6 Implementar stress opcional con condiciones de parada exactas del PRD.
  - Pasos: aumentar en 100k; validar cada paso; mantener histórico; detener ante error >0, lag creciente 3 pasos o memoria >85%, o al llegar a 5M.
  - Artefactos: stress runner y resultados por escalón.
  - Prueba: tests unitarios de cada condición de parada.
- [ ] B09.7 Asegurar limpieza aislada entre runs; no borrar volúmenes/datos de usuario ni resultados anteriores.
  - Pasos: aislar por runId; limitar borrado a topics de laboratorio creados por script; preservar benchmark-results; borrar solo temporales propios al cerrar.
  - Artefactos: RunIsolation/Cleanup.
  - Prueba: ejecutar dos runs y demostrar que el segundo no mezcla ni elimina el primero.
**Aceptación:** benchmark 1M ejecuta o deja blocker inequívoco; cifras reproducibles; cada run contiene todos los artefactos; datos validados antes de calcular throughput.

### B10 — Experiencia Windows/Linux, documentación y cierre
**Depende:** B02, B09.
- [ ] B10.1 Crear scripts start/stop/benchmark/full-suite/validate para Windows y shell.
  - Pasos: crear pares .bat/.sh; validar argumentos; cambiar al directorio raíz; propagar exit codes; imprimir ayuda breve; scripts idempotentes.
  - Artefactos: scripts/ y launchers raíz.
  - Prueba: invocar ayuda, start/stop y benchmark smoke desde Windows y shell cuando disponibles.
- [ ] B10.2 Actualizar RUN-KAFKA-LAB.bat: comprobar Git/Claude/Docker, iniciar Docker Desktop si existe, esperar disponibilidad con timeout, lanzar Claude una sola vez y devolver exit code.
  - Pasos: comprobar comandos; localizar Docker Desktop; iniciar si detenido; esperar máximo 120s; verificar PRD/BACKLOG; ejecutar Claude con instrucciones mínimas; preservar exit code y pausar solo al final.
  - Artefactos: RUN-KAFKA-LAB.bat.
  - Prueba: revisar rutas con espacios, Docker detenido/activo/no instalado y Claude ausente; no ejecutar permisos peligrosos sin advertencia visible.
- [ ] B10.3 Documentar prerequisitos, comandos, puertos, variables, troubleshooting y formatos de resultados en README.
  - Pasos: describir instalación exacta; quick start; parada; suites; endpoints; parámetros; problemas comunes; ruta de resultados.
  - Artefactos: README.md.
  - Prueba: persona con clon limpio puede seguir pasos sin consultar conversación.
- [ ] B10.4 Ejecutar mvnw clean verify, smoke e2e, benchmark 1M, validación independiente y --full-suite solo si se solicita.
  - Pasos: ejecutar build; unit/integration; compose; smoke pequeño; 1M; validador; suites extra solo si solicitadas; guardar comandos y exit codes.
  - Artefactos: informes y logs de ejecución.
  - Prueba: evidencias verificables; si el entorno bloquea, estado BLOCKED con causa concreta.
- [ ] B10.5 Revisar git diff, eliminar secretos/artefactos temporales, verificar scripts y dejar informe agregado reproducible.
  - Pasos: inspeccionar diff/status; buscar secretos; comprobar .gitignore; verificar scripts; resumir baseline/optimizado y limitaciones; commit final.
  - Artefactos: cambios fuente limpios e informe agregado.
  - Prueba: git diff --check; git status sin temporales; comandos documentados reproducibles.
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
Al terminar el ID padre, ejecutar su prueba de aceptación completa y actualizar TODO -> DONE o BLOCKED con evidencia. Esa actualización también debe quedar en un commit visible.

Ejemplos:
- `build(B01.1): add Spring Boot dependency management`
- `feat(B01.1): configure Kafka and actuator starters`
- `test(B03.2): verify deterministic message IDs`
- `fix(B05.3): acknowledge offsets after output confirmation`

## Protocolo de cierre por tarea
En cada checkbox sustituir TODO por DONE solo tras pasar aceptación; si no, BLOCKED con causa y evidencia. Registrar en una línea: `ID | estado | comando/prueba | resultado`. No pegar logs extensos en este archivo: guardar logs bajo benchmark-results. Si cambia alcance/arquitectura, actualizar primero PRD y este backlog en el mismo commit.

## Criterio global
No existe DONE global por tener código escrito. Solo al cumplir B10.5 con evidencias reales. Si el entorno impide ejecutar una prueba, marcarla BLOCKED, explicar el requisito externo y completar el resto.
