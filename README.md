# Kafka Performance Lab

**Windows:** ejecuta `RUN-KAFKA-LAB.bat`.

Requisitos: Git, Docker Desktop y Claude Code CLI instalado y autenticado. El launcher inicia Docker Desktop si hace falta y abre Claude; Claude implementa el proyecto, arranca Kafka KRaft con Docker Compose, prueba y ejecuta el benchmark de 1.000.000 de mensajes.

- [PRD.md](PRD.md): contrato funcional y técnico.
- [BACKLOG.md](BACKLOG.md): tareas ordenadas, dependencias y criterios de aceptación.
- [AGENTS.md](AGENTS.md): instrucciones canónicas para agentes de código.
- [CLAUDE.md](CLAUDE.md): punto de entrada compatible con Claude Code.
- `benchmark-results/`: evidencias e informes reales.

Puertos: Kafka 9094, app 8080, Prometheus 9090, Grafana 3000. No se deben inventar resultados; consulta los informes generados.