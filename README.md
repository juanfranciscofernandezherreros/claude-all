# Kafka Performance Lab

**Windows:** ejecuta `RUN-KAFKA-LAB.bat`.

Requisitos: Git, Docker Desktop y Claude Code CLI instalado y autenticado. El launcher abre Claude en el repositorio; Claude implementa el proyecto, inicia Kafka KRaft mediante Docker Compose, prueba y ejecuta el benchmark de 1.000.000 de mensajes. No necesitas instalar Kafka.

Especificación única: [PRD.md](PRD.md). Instrucciones del agente: [CLAUDE.md](CLAUDE.md). Informes reales: `benchmark-results/`.

Puertos: Kafka 9094, app 8080, Prometheus 9090, Grafana 3000. Nunca se deben inventar resultados: revisa los informes generados.