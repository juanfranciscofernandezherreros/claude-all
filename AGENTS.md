# AGENTS.md — Kafka Performance Lab

## Autoridad
- `PRD.md` es el contrato funcional/técnico único.
- `BACKLOG.md` es el plan obligatorio, con dependencias, micro-pasos, pruebas y commits.
- Este archivo define cómo debe trabajar el agente. Si hay contradicción, PRD prevalece en requisitos; BACKLOG prevalece en orden; informar y corregir documentación contradictoria antes de implementar.
- No ampliar alcance ni introducir tecnologías no previstas.

## Ejecución
1. Inspeccionar repo/entorno y comenzar por B00.
2. Ejecutar B00–B10 en orden; respetar dependencias y aceptación de cada tarea.
3. Cada micro-paso que cambie archivos requiere su propio commit atómico y visible, con ID de backlog en el mensaje.
4. Antes de cada commit: prueba focalizada, `git diff --check`, revisión de `git status`; no commits vacíos.
5. Al cerrar cada tarea padre, ejecutar aceptación completa y actualizar estado/evidencia en BACKLOG.md mediante commit visible.
6. No squash, amend de commits publicados, rebase interactivo ni force-push. Mantener historial lineal.
7. No tocar archivos ajenos ni borrar datos/volúmenes/resultados anteriores.


## Regla obligatoria: marcar siempre el BACKLOG
Esta regla es prioritaria y no se puede omitir:
- Cada vez que se complete una tarea o micro-paso del BACKLOG, actualizar inmediatamente `BACKLOG.md` y publicar esa actualización en un commit visible. No dejarlo para el final de la sesión.
- Cambiar siempre la casilla, no solo el texto:
  - `[x]` cuando la implementación de la tarea está completada y registrada, aunque falte ejecutar una verificación externa. En ese caso, el estado debe decir `DONE — pendiente de verificación` y nombrar la prueba pendiente.
  - `[ ]` cuando el trabajo sigue incompleto o está bloqueado por implementación/pruebas pendientes; indicar `BLOCKED` y la causa concreta.
  - `DONE` sin calificativo únicamente después de ejecutar y superar la prueba de aceptación.
- Al completar un micro-paso, registrar evidencia breve; al completar la tarea padre, sincronizar checkbox, estado y evidencia.
- Antes de cerrar cada sesión, revisar que todas las tareas completadas durante ella tengan `[x]`. Una tarea completada nunca debe quedarse accidentalmente con `[ ]`.

## Regla obligatoria de diseño SOLID
- Aplicar `docs/solid-principles.md` a toda funcionalidad nueva y a todo componente Java modificado de forma sustancial.
- Antes de implementar, inspeccionar responsabilidades, dependencias, variaciones esperadas y contratos existentes.
- Revisar SRP, OCP, LSP, ISP y DIP en cada cambio; marcar un principio como no aplicable cuando no exista una frontera o variación real.
- Mantener la arquitectura Spring Boot por capas y los paquetes existentes. No migrar a hexagonal, añadir microservicios ni crear interfaces por convención sin una necesidad demostrable y compatible con PRD.
- Preferir inyección por constructor y composición. Controladores/adaptadores traducen protocolos; los casos de uso coordinan; productores/consumidores aíslan el transporte; mappers no realizan I/O.
- Cada extracción/refactor debe preservar contratos funcionales, semántica de entrega Kafka y métricas; añadir pruebas de caracterización/contrato antes o junto al cambio.
- En el resumen de cada tarea, indicar decisiones SOLID relevantes, excepciones justificadas y pruebas ejecutadas. No declarar cumplimiento global sin auditoría y evidencias.

## Regla obligatoria: anotaciones Lombok y Spring
Aplicar estas convenciones al código Java nuevo y al que se modifique:
- Usar Lombok para boilerplate cuando reduzca código sin ocultar lógica: `@RequiredArgsConstructor` para constructor injection y `@Slf4j` para logging.
- Usar `@Builder(setterPrefix = "with")` cuando un builder aporte valor real; no introducir builders mecánicamente.
- Evitar `@Data`; preferir `@Getter` y `@Setter` explícitos según necesidad. Para POJOs de modelo simples puede usarse `@Getter`, `@Setter`, `@NoArgsConstructor`, `@AllArgsConstructor` y `@Builder(setterPrefix = "with")` solo si el contrato de serialización/mutabilidad lo permite.
- Las entidades JPA pueden usar anotaciones Lombok/JPA, pero no deben contener comportamiento de negocio.
- Prohibido `@Autowired` en producción: inyección por constructor. Inyección por campos solo se admite en tests existentes cuando simplifique fixtures; preferir constructor injection también en tests nuevos.
- Usar `@RestController` en controladores web y el mapping HTTP que corresponda al contrato: `@GetMapping`, `@PostMapping`, `@PutMapping`, `@PatchMapping` o `@DeleteMapping`.
- `@Service` se reserva para implementaciones de servicio `<Feature>ServiceImpl`; las interfaces, si existen por una frontera real, permanecen sin anotaciones. No crear interfaces solo para satisfacer esta convención.
- No implementar repositorios Spring Data estándar manualmente ni añadir `@Repository` innecesarios. Usar `@Component` para beans genéricos y `@Configuration` para configuración.
- Agrupar 3 o más propiedades relacionadas con `@ConfigurationProperties`; para menos propiedades puede usarse `@Value` cuando sea más simple y coherente con la configuración existente.
- Usar `@Transactional` a nivel de clase en implementaciones de servicio como límite transaccional por defecto. `@Transactional(readOnly = true)` a nivel de método es válido para sobrescribir explícitamente ese default en operaciones de lectura. No añadir transacciones a listeners/consumidores Kafka sin validar cuidadosamente sus efectos sobre ACK, offsets y EOS.
- Usar `@Validated` para validación de métodos/clases cuando proceda y `@RequestBody @Valid` para DTOs de escritura validados.
- Usar `@PreAuthorize` a nivel de controlador cuando la seguridad por método esté habilitada.
- Mantener dependencias acíclicas; no usar `@Order` para ocultar problemas de diseño.
- Aplicar excepciones justificadas cuando la convención choque con PRD, contrato Kafka, compatibilidad/Jackson, JPA o arquitectura actual; documentar el motivo en el cambio. No añadir Lombok ni Spring Security/JPA solo para usar estas anotaciones.

## Política del launcher Windows
- `RUN-KAFKA-LAB.bat` debe ser autónomo: detectar/iniciar Docker Desktop, esperar un máximo de 120 s, comprobar Compose v2 y ejecutar Docker Compose/benchmarks directamente.
- No invocar Claude/Claude Code ni añadir `--dangerously-skip-permissions`, `--allow-dangerous`, ni otros bypasses de permisos.
- Preservar el exit code del benchmark, soportar rutas con espacios (incluida la ruta de Git Bash) y limitar ACL de `benchmark-results` al usuario actual; nunca conceder escritura a `Everyone`.

## Calidad y seguridad
- No preguntar por decisiones ya cerradas. Para decisiones menores no especificadas, elegir opción simple compatible y documentar solo si afecta arquitectura.
- Nunca inventar pruebas, resultados, métricas, hardware o estado PASS/DONE.
- Ante bloqueo, registrar causa exacta, comando y evidencia; continuar trabajo independiente.
- Mantener secretos fuera del repositorio; no imprimirlos en logs.
- No usar imágenes/dependencias con etiqueta `latest`; fijar versiones.
- No afirmar exactly-once fuera de las transacciones Kafka.
- Validar integridad de mensajes independientemente del consumer antes de calcular throughput.

## Eficiencia de trabajo y tokens
- Leer PRD.md, BACKLOG.md y este archivo una vez al inicio.
- Después consultar solo archivos relevantes; no volver a volcar documentos completos.
- Agrupar razonamiento y llamadas de herramientas cuando sea posible, pero conservar commits separados por micro-paso.
- No narrar operaciones rutinarias ni generar resúmenes intermedios extensos.
- Respuesta final breve con commits, pruebas ejecutadas, resultados reales y bloqueos.

## Convención de commits
Formato: `tipo(ID): verbo + objeto`.
Tipos: `feat`, `fix`, `test`, `build`, `docs`, `refactor`, `chore`.
Ejemplos:
- `build(B01.1): add Spring Boot dependency management`
- `feat(B04.2): add deterministic message ID generator`
- `test(B03.2): verify deterministic message IDs`
- `fix(B05.3): acknowledge offsets after output confirmation`

Los cambios funcionales, sus tests y la documentación de cada micro-paso se separan en commits distintos cuando sean acciones independientes. Comprobaciones sin cambios en archivos se registran como evidencia, nunca con commits vacíos.
