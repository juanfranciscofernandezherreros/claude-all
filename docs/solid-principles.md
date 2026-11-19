# SOLID design principles for Spring Boot

This document is the canonical source of object-oriented design rules based on SOLID for Kafka Performance Lab. It applies to the existing layered Spring Boot application and complements, rather than replaces, the PRD and repository workflow rules.

## Purpose

Keep application code cohesive, loosely coupled, extensible, substitutable, and testable. Apply the principles pragmatically: do not add interfaces, layers, indirection, or design patterns without a concrete need.

## S — Single Responsibility Principle

A class or module should have one cohesive responsibility and one primary reason to change.

- Keep HTTP translation in controllers/adapters; delegate use cases to application services.
- Keep business decisions in the application layer and domain policy in focused components.
- Keep persistence, external I/O, message transport, serialization, and mapping concerns at their defined boundaries.
- Extract a collaborator when it owns an independently changing policy or responsibility, not merely to reduce line count.
- Avoid services that combine unrelated workflows or infrastructure concerns.

## O — Open/Closed Principle

Make stable behavior extensible without repeatedly editing a central conditional or stable component.

- Use polymorphism, strategies, or composed policies when multiple variants are real or reasonably expected.
- Keep variation behind a narrow contract and select implementations at a composition/configuration boundary.
- Prefer a simple conditional for a small, closed, stable set of cases; OCP is not a mandate to create an interface for every branch.
- When adding a variant, avoid modifying unrelated implementations and preserve existing behavior.

## L — Liskov Substitution Principle

An implementation must honor the behavioral contract of its abstraction.

- Preserve documented preconditions, postconditions, invariants, error semantics, and side-effect expectations.
- Do not strengthen caller preconditions, weaken guarantees, or surprise callers with unsupported operations.
- Do not use inheritance or interface implementation merely to share code when the types are not behaviorally substitutable.
- Test implementations against the same contract where multiple implementations exist.

## I — Interface Segregation Principle

Clients should depend only on the operations they actually need.

- Define small, cohesive ports and interfaces around consumer needs.
- Separate read and write capabilities when consumers have different permissions or responsibilities.
- Avoid broad interfaces that force implementations to provide meaningless methods.
- Do not split interfaces into tiny fragments without a real difference in clients or change cadence.

## D — Dependency Inversion Principle

High-level policy should not depend directly on low-level infrastructure details; both should meet at stable abstractions.

- Keep application policy independent of concrete brokers, filesystem APIs, HTTP clients, and framework adapters where a meaningful boundary exists.
- Use constructor injection for Spring-managed collaborators.
- Introduce interfaces at meaningful variation, testing, or ownership boundaries rather than mechanically for every service/repository.
- Avoid constructing infrastructure clients/repositories inside business logic.
- Preserve the existing layered package conventions; do not migrate to hexagonal architecture or add remote services unless the PRD explicitly changes.

## Spring Boot implementation guidance

- Controllers handle protocol concerns and delegate; they must not contain business workflows or persistence logic.
- Application services coordinate use cases and transactions.
- Repositories/adapters perform I/O; they should not become a second home for business policy.
- Mappers perform deterministic transformations and must not perform I/O or hide business workflows.
- HTTP clients and message producers/consumers isolate transport concerns from application policy.
- Prefer constructor injection. Avoid field injection and service-locator patterns.
- Use interfaces where they define a meaningful port, public contract, substitution point, or independently varying implementation. Do not create an interface/implementation pair solely by convention.
- Prefer composition over inheritance when it better expresses the relationship.

## Required review checklist

For each new or materially changed component, review the applicable questions:

- [ ] SRP: Does this component have one cohesive responsibility?
- [ ] OCP: Can expected variants be added without invasive changes to stable code?
- [ ] LSP: Do implementations honor the complete behavioral contract?
- [ ] ISP: Do consumers depend only on the capabilities they need?
- [ ] DIP: Are policy and business rules insulated from concrete infrastructure where a meaningful boundary exists?
- [ ] Architecture: Are dependencies consistent with the existing layered Spring Boot structure?
- [ ] Tests: Are business behavior and relevant adapter contracts covered?

Mark a principle **not applicable** when there is no meaningful variation, substitution, or boundary to protect. Do not manufacture abstractions to make every checkbox appear positive.

## Agent workflow

When generating or changing code:

1. Read this document alongside AGENTS.md and PRD.md.
2. Inspect existing project conventions before proposing new interfaces, base classes, packages, or patterns.
3. Identify responsibilities, dependency direction, expected variation, and contracts before implementation.
4. Implement the smallest design that satisfies the requirement and preserves the existing architecture.
5. Add or update tests for business behavior and any interchangeable implementation contracts.
6. In the completion summary, call out material SOLID decisions and deliberate exceptions; do not claim formal compliance solely because interfaces were introduced.

## Relationship to other standards

- AGENTS.md remains the root source for agent workflow and rule precedence.
- PRD.md remains authoritative for functional and technical requirements.
- BACKLOG.md remains authoritative for task order, acceptance criteria, and evidence.
- This document defines SOLID design guidance and must not redefine those other standards.
