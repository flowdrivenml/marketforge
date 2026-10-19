# MarketForge Live — AI-Assisted Development Workflow

## 1. Objective

Develop MarketForge Live using AI coding agents while minimizing manual involvement, unnecessary rework, and implementation errors.

The development process must:

- Reuse the existing MarketForge historical engine.
- Preserve approved architectural decisions.
- Correct documentation inconsistencies before implementation.
- Implement complete module requirements rather than incomplete prototypes.
- Avoid repeatedly revisiting modules to add functionality that was already planned.
- Automate code generation, compilation, testing, and ordinary debugging.
- Preserve implementation progress across AI context resets.
- Support autonomous development on local hardware.
- Require explicit verification before declaring modules complete.

The objective is not to eliminate human supervision entirely.

The objective is to shift human involvement from routine programming tasks to architectural decisions, exceptional problems, and final verification.

---

## 2. Development Environment

### 2.1 Hardware

Available local hardware:

| Component | Specification |
|---|---|
| GPU | NVIDIA RTX 5090 |
| GPU VRAM | 32 GB |
| System RAM | 128 GB |
| Operating system | Ubuntu/Linux |
| Local inference | Ollama |
| Preferred models | Qwen coding/reasoning models |
| Target context | Approximately 64K–76K tokens |

The exact coding model will be selected through practical testing.

The preferred model should offer:

- Reliable tool calling.
- Strong Rust programming ability.
- Multi-file editing.
- Codebase navigation.
- Compiler-error interpretation.
- Autonomous debugging.
- Reasoning across module interfaces.
- Good performance within available GPU memory.

### 2.2 Software Stack

| Component | Tool | Responsibility |
|---|---|---|
| Inference | Ollama | Execute local language models |
| Coding agent | OpenCode | Read, write, compile, and test code |
| Specification management | OpenSpec | Produce structured requirements and implementation plans |
| Autonomous execution | Ralph | Execute implementation tasks across repeated AI sessions |
| Source control | Git | Versioning, isolated branches, checkpoints |
| Rust build system | Cargo | Compilation and testing |
| Project instructions | AGENTS.md | Persistent rules for AI agents |
| Verification | Automated test runner | Independent acceptance testing |

Primary repositories:

**OpenCode**

https://github.com/anomalyco/opencode

**OpenSpec**

https://github.com/Fission-AI/OpenSpec

**Ralph — OpenCode-Compatible Implementation**

https://github.com/iannuttall/ralph

**Original Ralph Reference**

https://github.com/snarktank/ralph

Important:

The original `snarktank/ralph` implementation currently targets Amp and Claude Code rather than OpenCode directly.

For the initial local workflow, prefer an OpenCode-compatible Ralph implementation.

---

## 3. Overall Development Architecture

```text
                    HUMAN
                      |
                      v
          EXISTING ARCHITECTURE
               Documents 00–16
                      |
                      v
          DOCUMENTATION CORRECTION
               22-Step Plan
                      |
                      v
               OpenSpec
            Documentation Proposals
                      |
                      v
           OpenCode + Ollama
           Apply Approved Fixes
                      |
                      v
            CONSISTENT ARCHITECTURE
                      |
                      v
          HISTORICAL CODEBASE AUDIT
                      |
                      v
              Code Reuse Map
                      |
                      v
             Document 17
          Implementation Roadmap
                      |
                      v
                 OpenSpec
          Complete Module Work Packages
                      |
                      v
              Human Approval
                      |
                      v
                  Ralph
                      |
                      v
                 OpenCode
                      |
                      v
              Ollama + Qwen
                      |
                      v
          Implementation / Compilation
                      |
                      v
            Automated Test / Repair
                      |
                      v
          Independent Acceptance Tests
                      |
                +-----+-----+
                |           |
                v           v
             VERIFIED     BLOCKED
                |           |
                v           v
             Commit       Report
                |
                v
          NEXT WORK PACKAGE
```

---

# PHASE 1 — Prepare the Repository

## 4. Organize Development Documentation

All architecture documents must be available to AI agents through the local repository.

Suggested structure:

```text
marketforge/
│
├── AGENTS.md
│
├── docs/
│   ├── live/
│   │   ├── 00 — Master Plan.md
│   │   ├── 01 — Architecture and Module Boundaries.md
│   │   ├── ...
│   │   ├── 16 — External Libraries and Code Reuse.md
│   │   └── 17 — Implementation Roadmap.md
│   │
│   └── planning/
│       └── documentation-correction-plan.md
│
├── openspec/
│
├── engine/
│   └── src/
│
└── ...
```

The structure is illustrative and should be adapted to the existing MarketForge repository.

### Required Inputs

1. Documents 00–16.
2. The 22-step Documentation Consistency and Implementation Readiness Plan.
3. Existing historical MarketForge source code.
4. Existing Cargo manifests.
5. Existing tests.
6. Existing project configuration and database contracts.

### Important Rule

Documentation and implementation source code must remain distinguishable.

Architectural descriptions are not proof that a particular Rust function, type, or dependency already exists.

Existing functionality must be verified through source-code inspection.

---

## 5. Configure OpenCode

Configure OpenCode to communicate with the selected Ollama model.

Before introducing autonomous execution, verify:

- OpenCode can connect to Ollama.
- The model supports reliable tool calls.
- OpenCode can inspect repository files.
- OpenCode can create and edit files.
- OpenCode can execute Cargo commands.
- OpenCode can interpret compiler errors.
- OpenCode can repair ordinary implementation defects.

Begin with interactive or single-task execution.

Do not enable long autonomous runs before basic tool reliability has been established.

---

## 6. Configure OpenSpec

Install OpenSpec and initialize it within the MarketForge repository.

OpenSpec will be used for two different purposes.

### Purpose A — Documentation Maintenance

Transform the existing correction plan into controlled documentation changes.

### Purpose B — Implementation Planning

Transform finalized module specifications into complete implementation work packages.

These activities must remain separate.

OpenSpec must not independently redefine the approved MarketForge architecture.

---

## 7. Create AGENTS.md

Create one concise project-level instruction file.

Its responsibilities include:

- Identifying authoritative documentation.
- Explaining repository structure.
- Establishing coding conventions.
- Defining module ownership rules.
- Requiring reuse of historical functionality.
- Prohibiting duplicate canonical models.
- Establishing testing requirements.
- Preventing unapproved architectural changes.
- Defining escalation conditions.
- Defining implementation-completion criteria.

The instructions should be concise.

Detailed requirements remain in their respective architecture and implementation documents.

---

# PHASE 2 — Correct Architecture Documentation

## 8. Use the Existing Correction Plan

The architecture correction plan already contains 22 steps.

We must not ask OpenSpec to independently invent another documentation strategy.

Instead, use the existing plan as the authoritative correction backlog.

### Workflow

```text
Documentation Correction Step
             |
             v
          OpenSpec
             |
             v
      Change Proposal
             |
             v
      Identify Affected Files
             |
             v
      Review Existing Contracts
             |
             v
      Proposed Corrections
             |
             v
        Human Approval
             |
             v
      OpenCode Applies Changes
             |
             v
      Cross-Document Verification
             |
             v
      Commit Documentation Changes
```

## 9. Documentation Correction Rules

AI agents must:

1. Preserve approved architecture decisions.
2. Identify contradictory requirements.
3. Identify outdated examples.
4. Respect authoritative document ownership.
5. Update affected cross-references.
6. Avoid introducing new features without approval.
7. Avoid rewriting unrelated sections.
8. Report unresolved architectural decisions.
9. Verify that dependent documents remain consistent.
10. Record every approved correction.

### Important

When two documents contradict each other, the AI must not arbitrarily choose one.

It must identify the conflict.

If an authoritative decision already exists, the AI should apply it consistently.

If the decision remains unresolved, human approval is required.

---

## 10. Execute Corrections Incrementally

Do not instruct the AI to rewrite all documentation in one enormous operation.

Execute the 22-step plan through bounded correction packages.

Each package should contain:

    Correction ID

    Objective

    Affected Documents

    Existing Contradictions

    Authoritative Decisions

    Required Changes

    Forbidden Changes

    Verification Criteria

    Completion Conditions

Corrections may be automated once the process proves reliable.

Architectural decisions remain subject to human review.

---

## 11. Documentation Completion Gate

Documentation correction is complete when:

- All known implementation-critical contradictions are resolved.
- Document references are consistent.
- Shared terminology is standardized.
- Ownership boundaries are unambiguous.
- Public contracts are compatible.
- No obsolete example competes with an authoritative contract.
- Unresolved requirements are explicitly documented.
- Required implementation decisions are finalized or assigned
  to a specific pre-implementation gate.

Only then should the project proceed toward autonomous production coding.

---

# PHASE 3 — Audit the Existing Historical Engine

## 12. Inspect the Actual Source Code

Before implementing MarketForge Live, OpenCode must inspect the existing historical codebase.

The objective is to prevent unnecessary redevelopment of functionality that already exists.

### Required Reuse Candidates

    Canonical Trade Models

    Canonical L2 Models

    Exact Decimal Arithmetic

    Timestamp Normalization

    Instrument Metadata

    BookStore

    Sequence Validation

    Historical Depth Grouping

    Arrow / Parquet Infrastructure

    Historical Merge Ordering

    Currency Conversion

    Ratatui Components

    Tokyo Night Theme

    Existing Test Fixtures

### Required Output

Produce a code reuse map.

For each reusable component, identify:

| Field | Description |
|---|---|
| Component | Functionality being evaluated |
| Source path | Actual file location |
| Existing types | Relevant Rust structures and enums |
| Existing functions | Reusable implementation |
| Existing tests | Available verification |
| Dependencies | Required crates and modules |
| Reuse decision | Reuse, adapt, reference, or implement |
| Risks | Compatibility or architectural concerns |

### Reuse Classifications

    Reuse Unchanged

    Reuse With Adaptation

    Reference Only

    Implement New

The historical engine remains the authoritative source for compatible canonical financial semantics.

---

# PHASE 4 — Finalize the Implementation Roadmap

## 13. Create Document 17

After the documentation corrections and historical code audit, finalize:

    17 — Implementation Roadmap.md

This will be the final architecture document.

It must describe:

- Implementation phases.
- Module dependencies.
- Execution order.
- Complete module responsibilities.
- Public interface requirements.
- Existing code reuse.
- Integration milestones.
- Required tests.
- Acceptance criteria.
- Autonomous execution boundaries.

### Implementation Philosophy

    Contract-First

    Test-First

    Complete Approved Module Scope

    Incremental Integration

    Minimal Avoidable Backtracking

    Automated Verification

### Important Distinction

The implementation roadmap defines:

    WHAT TO IMPLEMENT

    IN WHICH ORDER

    WITH WHICH DEPENDENCIES

    UNDER WHICH ACCEPTANCE CRITERIA

OpenSpec subsequently converts the approved roadmap
into agent-executable module work packages.

---

# PHASE 5 — Generate Complete Module Work Packages

## 14. Use OpenSpec for Implementation Planning

For every module selected by Document 17:

```text
Approved Module Specification
             |
             v
           OpenSpec
             |
      +------+------+------+
      |      |      |      |
      v      v      v      v
 Requirements Design Tasks Acceptance
                          Tests
      |      |      |      |
      +------+------+------+
             |
             v
      Complete Work Package
```

### Required Deliverables

Each package must include:

1. Module objective.
2. Complete approved v1 scope.
3. Authoritative architecture references.
4. Existing code reuse map.
5. Public interfaces.
6. Internal responsibilities.
7. Upstream dependencies.
8. Downstream consumers.
9. Configuration requirements.
10. Metrics and diagnostics.
11. Error and recovery behavior.
12. Resource constraints.
13. Unit tests.
14. Integration tests.
15. Failure-injection tests.
16. Historical regression requirements.
17. Explicit exclusions.
18. Completion criteria.

## 15. Prevent Avoidable Backtracking

The module specification must cover all functionality already required by its approved v1 architecture.

Example:

    Connection Manager
           |
           +-- Connection Pooling
           |
           +-- Transport Reader/Writer
           |
           +-- Heartbeat Handling
           |
           +-- Connection Generations
           |
           +-- Reconnection
           |
           +-- Rate-Limit Integration
           |
           +-- Cancellation
           |
           +-- Resource Limits
           |
           +-- Shutdown
           |
           +-- Monitoring
           |
           +-- Fault-Injection Tests

The complete package may contain many small implementation tasks.

However, the module is not complete until every approved requirement is satisfied.

### Allowed Revisions

Returning to a completed module is permitted for:

- Discovered defects.
- Correctness improvements.
- Measured performance improvements.
- Security fixes.
- Approved architecture changes.

It should not be necessary merely to add functionality that was explicitly required before implementation began.

---

# PHASE 6 — Autonomous Implementation With Ralph

## 16. Introduce Ralph

Ralph is responsible for repeatedly executing approved coding tasks.

Its role is to automate implementation progress across multiple AI sessions.

### Execution Architecture

```text
           Approved Work Package
                    |
                    v
             Ralph Task Queue
                    |
                    v
                 OpenCode
                    |
                    v
               Ollama / Qwen
                    |
                    v
              Implement Task
                    |
                    v
               Compile Rust
                    |
                    v
                Run Tests
                    |
              +-----+-----+
              |           |
              v           v
            PASS        FAILURE
              |           |
              |           v
              |         Repair
              |           |
              +-----<-----+
                    |
                    v
              Record Progress
                    |
                    v
               Next Task
```

## 17. Integrate OpenSpec With Ralph

OpenSpec and Ralph are independent projects.

They do not automatically share one native task format.

A conversion step is required:

    OpenSpec Requirements
             |
             v
    OpenSpec Implementation Tasks
             |
             v
    Task Format Validation
             |
             v
    Ralph-Compatible Task File
             |
             v
    Ralph Execution

The conversion must preserve:

- Requirement identifiers.
- Task dependencies.
- Implementation scope.
- Acceptance criteria.
- Completion state.

It must not silently remove requirements.

## 18. Persistent Agent Progress

Progress must survive:

    Agent Session Termination

    Context Window Reset

    Model Restart

    Process Interruption

    Computer Restart

Persistent information should include:

    Current Work Package

    Completed Tasks

    Remaining Tasks

    Git Commits

    Failed Tests

    Previous Attempts

    Important Implementation Findings

    Unresolved Problems

The system must not depend on one continuously growing chat history.

---

# PHASE 7 — Independent Verification

## 19. Automated Test Execution

AI coding agents are allowed to:

- Write code.
- Run compilation.
- Run tests.
- Inspect compiler errors.
- Repair implementation defects.
- Refactor within approved boundaries.

However, they must not independently redefine completion criteria.

The acceptance suite must be established before implementation.

### Required Verification

Depending on the module:

    cargo fmt --check

    cargo clippy

    cargo test

    Contract Tests

    Integration Tests

    Fault-Injection Tests

    Historical Regression Tests

    Performance Benchmarks

Actual commands must be verified against the existing Rust workspace.

## 20. Independent Acceptance Gate

A module is complete only when:

1. All approved requirements are implemented.
2. Required tests pass.
3. Integration interfaces are compatible.
4. Failure behavior is verified.
5. Historical functionality remains correct.
6. No unapproved architecture changes occurred.
7. No required functionality was silently deferred.
8. Verification results are recorded.

The coding agent's own completion declaration is insufficient.

### Verification Outcomes

    VERIFIED

    VERIFICATION_FAILED

    BLOCKED

    REQUIRES_APPROVAL

Only verified work is eligible for final acceptance.

---

# PHASE 8 — Autonomy and Safety

## 21. Agent Permissions

Agents may autonomously:

- Inspect the repository.
- Modify approved files.
- Implement approved tasks.
- Run development commands.
- Fix compiler errors.
- Repair test failures.
- Create tests.
- Make implementation commits.
- Record progress.

Agents require approval before:

- Changing frozen public contracts.
- Altering canonical financial semantics.
- Weakening integrity guarantees.
- Weakening durability guarantees.
- Modifying unrelated completed modules.
- Introducing substantial unapproved dependencies.
- Deleting acceptance tests.
- Accessing production data or credentials.

## 22. Isolated Development

Autonomous execution should use:

    Separate Git Worktree

    Dedicated Development Branch

    Isolated Test Database

    Temporary Test Directories

    Restricted Credentials

    Bounded Execution Resources

Production databases, exchange credentials, and valuable historical datasets must remain protected.

## 23. Failure Handling

If an agent cannot complete its task:

    Stop After Configured Retry Budget

    Preserve Existing Work

    Record Failure Details

    Record Attempted Solutions

    Identify Blocking Requirement

    Report Blocked Status

The agent must not silently simplify or abandon required functionality.

---

# PHASE 9 — Evaluate the Local Model

## 24. Initial Model Evaluation

Before allowing long autonomous execution, evaluate the selected Qwen model on representative MarketForge tasks.

### Experiment A — Deterministic Implementation

Example:

    Metrics Registry

Evaluate:

- Requirement coverage.
- Rust correctness.
- Tool reliability.
- Test quality.
- Resource safety.
- Autonomous completion.

### Experiment B — Concurrent State Management

Example:

    Stream Registry Lifecycle

Evaluate:

- State transitions.
- Generation fencing.
- Cancellation.
- Race conditions.
- Resource cleanup.
- Failure recovery.

### Experiment C — End-to-End Integration

Example:

    Fake Exchange
         |
         v
    Protocol Decoder
         |
         v
    Normalization
         |
         v
    Integrity Validation
         |
         v
    Event Router
         |
         v
    Test Subscriber

Evaluate the model's ability to integrate several components.

## 25. Model Selection Criteria

Evaluate models according to:

    Accepted Work Packages

    Independent Test Pass Rate

    Architectural Compliance

    Number Of Repair Iterations

    Human Interventions

    Runtime

    GPU Memory Usage

    Tool-Calling Reliability

    Resource Consumption

A larger model is not automatically better.

A smaller model that produces more verified work per hour may be preferable.

### Initial Strategy

Start with one local coding agent.

Introduce multiple agents only after the single-agent workflow is reliable.

A stronger hosted model may be used selectively for difficult architectural or correctness problems.

---

# PHASE 10 — Full Development Workflow

## 26. Final Operating Model

After setup, ordinary implementation should work as follows.

### Human Responsibilities

    Approve Architecture

    Approve Module Scope

    Resolve Critical Decisions

    Review Significant Changes

    Accept Verified Milestones

### OpenSpec Responsibilities

    Organize Requirements

    Prepare Module Designs

    Generate Implementation Tasks

    Track Specification Changes

### Ralph Responsibilities

    Select Tasks

    Launch Coding Sessions

    Preserve Progress

    Continue Implementation

    Stop At Completion Or Blocker

### OpenCode Responsibilities

    Inspect Existing Code

    Implement Rust

    Write Tests

    Compile

    Debug

    Refactor

### Ollama Responsibilities

    Execute Local AI Models

### Verification System Responsibilities

    Enforce Acceptance Criteria

    Run Required Tests

    Detect Regressions

    Produce Verification Evidence

---

# 27. Practical Setup Order

Do not configure the entire autonomous development stack simultaneously.

| Order | Action | Expected Result |
|---|---|---|
| 1 | Add architecture documents to Git | Version-controlled specifications |
| 2 | Add the documentation correction plan | Authoritative correction backlog |
| 3 | Configure OpenCode with Ollama | Working local coding agent |
| 4 | Initialize OpenSpec | Structured specification workflow |
| 5 | Create AGENTS.md | Permanent agent instructions |
| 6 | Test one documentation correction | Validate agent compliance |
| 7 | Complete documentation corrections | Consistent architecture |
| 8 | Inspect historical Rust implementation | Verified reuse map |
| 9 | Finalize Document 17 | Complete implementation roadmap |
| 10 | Generate one module package with OpenSpec | Approved requirements and tasks |
| 11 | Configure OpenCode-compatible Ralph | Autonomous execution loop |
| 12 | Integrate OpenSpec tasks with Ralph | Reliable task handoff |
| 13 | Run one bounded implementation experiment | Test local model capability |
| 14 | Verify results independently | Measure correctness and autonomy |
| 15 | Improve agent configuration | Remove observed workflow problems |
| 16 | Begin sequential module implementation | Autonomous development |
| 17 | Introduce parallel execution if justified | Higher throughput |

---

# 28. Final Principles

## Architecture Before Implementation

Do not implement against contradictory specifications.

## Reuse Before Reimplementation

Inspect existing MarketForge source code before implementing new domain functionality.

## Complete Scope Before Module Acceptance

Every approved v1 module requirement must be addressed before the module is declared complete.

## Small Tasks, Complete Modules

Individual AI tasks should remain small enough for reliable execution.

Their combined scope must cover the complete approved module.

## Independent Verification

Working code must be verified by deterministic tests and established acceptance criteria.

## Persistent Progress

Implementation progress must be preserved outside the model's context window.

## Controlled Autonomy

AI agents may independently solve ordinary programming problems.

Architectural changes require explicit authorization.

## Tool Independence

The process should work with local Ollama models and remain adaptable to hosted coding models.

The architecture and implementation instructions must not depend on one coding-tool vendor.

---

# 29. Current Project Status

| Area | Status |
|---|---|
| MarketForge historical engine | Existing |
| MarketForge Live architecture documents 00–16 | Written |
| Documentation consistency audit | Initial audit completed |
| Documentation correction plan | Prepared |
| Documentation corrections | Not yet applied |
| OpenCode configuration | Not yet established for this workflow |
| OpenSpec initialization | Not yet completed |
| AGENTS.md | Not yet created |
| Historical implementation reuse audit | Pending |
| Document 17 — Implementation Roadmap | Pending |
| Ralph configuration | Pending |
| Autonomous coding experiment | Pending |
| MarketForge Live implementation | Pending |

---

# 30. Immediate Next Action

Begin with:

    PHASE 1 — Repository Preparation

Specifically:

1. Configure OpenCode with Ollama.
2. Initialize OpenSpec in the MarketForge repository.
3. Prepare the initial AGENTS.md.
4. Make the architecture documentation and correction plan accessible.
5. Run one controlled documentation-correction experiment.

Do not start Ralph or autonomous Rust implementation yet.

First establish that OpenCode and the selected local model can reliably follow project instructions and preserve architectural decisions.

---

# Final Objective

    Complete MarketForge Architecture
                  |
                  v
        Consistent Specifications
                  |
                  v
         Verified Historical Reuse
                  |
                  v
          Complete Module Plans
                  |
                  v
        Autonomous AI Implementation
                  |
                  v
          Compilation And Testing
                  |
                  v
          Independent Verification
                  |
                  v
             Accepted Modules
                  |
                  v
          Completed MarketForge Live

**The human defines what correct software means.**

**The AI performs most of the work required to implement it.**

**The verification system determines whether that work satisfies the approved requirements.**