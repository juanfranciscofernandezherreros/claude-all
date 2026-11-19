# Kafka Performance Lab

Laboratorio local para generar, transformar, validar y medir mensajes Kafka con Java 21 y Spring Boot 3.4.5. El broker Kafka 3.9.0 arranca en KRaft mediante Docker Compose; no necesita ZooKeeper ni una instalación local de Kafka.

## Inicio rápido

### Windows

1. Instala Docker Desktop con Docker Compose v2, Git for Windows (incluye Git Bash) y Python 3.
2. Clona este repositorio.
3. Ejecuta `RUN-KAFKA-LAB.bat`.

El launcher inicia Docker Desktop si está instalado pero detenido, levanta el stack y ejecuta el benchmark principal de 1.000.000 de mensajes. Para levantar solo los servicios: `scripts\start.bat`.

### Linux

Requisitos: Docker Engine + Compose v2, Git, Bash y Python 3.10 o posterior. Para compilar fuera de Docker, también Java 21.

```bash
docker compose up -d --build
bash scripts/run-benchmark.sh 1000000
```

Parar los servicios sin borrar volúmenes ni resultados:

```bash
bash scripts/stop.sh
```

No uses `docker compose down -v` salvo que quieras borrar expresamente los datos persistentes de Kafka, Prometheus y Grafana.

## Comandos

| Acción | Windows | Linux |
|---|---|---|
| Arrancar stack | `scripts\start.bat` | `bash scripts/start.sh` |
| Parar stack | `scripts\stop.bat` | `bash scripts/stop.sh` |
| Generar mensajes | `scripts\generate.bat 10000 smoke-001` | `bash scripts/generate.sh 10000 smoke-001` |
| Validar ejecución | `scripts\validate.bat smoke-001 10000` | `bash scripts/validate.sh smoke-001 10000` |
| Benchmark principal | `scripts\run-benchmark.bat 1000000` | `bash scripts/run-benchmark.sh 1000000` |
| Suite completa | `scripts\run-full-suite.bat` | `bash scripts/run-full-suite.sh` |

El benchmark principal ejecuta un warm-up de 10.000 mensajes y tres repeticiones medidas para concurrencias 1, 2, 5, 10 y 20. La suite completa añade escenarios de payload (1/5/10/50/100 KiB), carga LIGHT/MEDIUM/HEAVY y entrega AT_LEAST_ONCE/EXACTLY_ONCE. Los escenarios secundarios usan 100.000 mensajes por repetición por defecto. Puedes cambiarlo con `SECONDARY_MESSAGES`.

Para compilar y probar desde el host:

- Windows: `mvnw.cmd clean verify`
- Linux/macOS: `bash ./mvnw -B clean verify`

El wrapper descarga Maven 3.9.9 en el perfil local del usuario. GitHub Actions ejecuta la misma verificación con Java 21 y valida la configuración de Compose.

## Arquitectura y flujo

- `kafka`: Apache Kafka 3.9.0, KRaft, un broker, 24 particiones, replication factor 1.
- `app`: Spring Boot, consumer `kafka-lab`, productor de salida, generador y validador independientes.
- `prometheus`: scrape de Actuator cada 5 segundos.
- `grafana`: dashboard provisionado automáticamente.

Flujo: generador → `input-topic` → consumer → SHA-256/workload → `output-topic`. Mensajes inválidos o fallos permanentes se contabilizan en `input-dlt-topic`. El consumer espera el ACK del productor antes de confirmar offsets en AT_LEAST_ONCE. En EXACTLY_ONCE, salida y offset se publican en una transacción Kafka y los lectores usan `read_committed`.

El validador es un proceso separado: captura offsets de inicio, lee solo el rango de la ejecución y compara secuencias, duplicados, ausencias, extras, esquema y checksum contra los mensajes de entrada. La memoria de checksum se acota al número esperado de mensajes.

## Puertos locales

Todos los puertos se enlazan a loopback (127.0.0.1), no a la red externa:

| Servicio | URL |
|---|---|
| Kafka externo | `localhost:9094` |
| API / Actuator | `http://localhost:8080/actuator/health` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |

Grafana local: usuario `admin`, contraseña inicial `admin`. Cambia la contraseña si modificas el alcance de red o expones el servicio.

## Configuración

Copia `.env.example` a `.env` y modifica solo lo necesario. Los valores predeterminados cumplen el PRD.

| Variable | Predeterminado | Descripción |
|---|---:|---|
| `MESSAGES` | 1000000 | Mensajes por ejecución principal |
| `PARTITIONS` | 24 | Particiones Kafka (fijo) |
| `CONCURRENCY` | 1 | 1, 2, 5, 10 o 20 |
| `BATCH_SIZE` | 500 | Registros por poll y bloque lógico del generador |
| `PRODUCER_BATCH_BYTES` | 65536 | `batch.size` Kafka en bytes |
| `GENERATOR_THREADS` | 4 | Hilos del generador |
| `PAYLOAD_BYTES` | 1024 | 1/5/10/50/100 KiB expresados en bytes |
| `DELIVERY_MODE` | AT_LEAST_ONCE | AT_LEAST_ONCE o EXACTLY_ONCE |
| `WORKLOAD` | LIGHT | LIGHT=1, MEDIUM=10, HEAVY=100 rondas SHA-256 |
| `COMPRESSION_TYPE` | none | Compresión del productor Kafka |
| `LINGER_MS` | 5 | Espera del productor en milisegundos |

No se usan sleeps artificiales en el procesamiento. Las variables se validan al arrancar.

## Resultados

Cada ejecución medida crea `benchmark-results/<runId>/` con:

- `summary.json`: configuración, tiempos, throughput validado y latencias.
- `results.csv`: tabla agregada de escenarios.
- `validation.json`: integridad independiente (expected, valid, missing, duplicate, unexpected, invalid y DLT).
- `environment.json`: SO, arquitectura, CPU lógica, Java y Docker; los recursos no capturados aparecen como null con explicación.
- `execution.log`: salida del generador.
- `report.md`: resumen legible.

Los offsets auxiliares y logs temporales de warm-up están ignorados por Git. Los datos persistentes de Kafka y los informes anteriores no se borran entre ejecuciones.

## Métricas y dashboard

Prometheus recoge métricas de entradas, salidas, fallos, reintentos, DLT, procesamiento, publicación, latencia end-to-end y JVM. Grafana carga el dashboard automáticamente. El lag de Kafka solo aparecerá si el runtime/exporter lo expone.

## Troubleshooting

- **Docker no responde:** abre Docker Desktop y espera a que el Engine esté listo; vuelve a ejecutar el launcher.
- **Puerto ocupado:** libera 9094, 8080, 9090 o 3000 y reinicia Compose.
- **La app no arranca:** consulta `docker compose logs app kafka`.
- **Kafka no está healthy:** consulta `docker compose logs kafka`; no borres el volumen como primera medida.
- **Falla la validación:** conserva `execution.log`, `validation.json` y los logs de Compose; no interpretes un benchmark con integridad FAIL como throughput válido.
- **Falta Python:** instala Python 3.10+ y asegúrate de que `python3` o `python` está en PATH.

## Documentos

- [PRD.md](PRD.md): contrato funcional y técnico.
- [BACKLOG.md](BACKLOG.md): tareas, dependencias, pruebas y evidencias.
- [AGENTS.md](AGENTS.md): guía canónica para agentes.
- [CLAUDE.md](CLAUDE.md): punto de entrada de compatibilidad.
