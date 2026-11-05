# Contributing to Thryve

Thank you for your interest in contributing to Thryve! This document provides guidelines and instructions for contributing to the project.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Code Style](#code-style)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [CLA (Contributor License Agreement)](#cla-contributor-license-agreement)

---

## Code of Conduct

- Be respectful and inclusive in all interactions
- Provide constructive feedback
- Accept constructive criticism gracefully
- Focus on what's best for the community

---

## Getting Started

### Prerequisites

- **Windows** - Thryve builds on Windows only, for now.
- **Rust, latest stable** - [Install Rust](https://rustup.rs/). Nothing older than 1.98 can build it: the code uses `Path::is_empty`, new in 1.98.
- **Git** - Version control
- **GitHub account** - For submitting pull requests

### Clone the Repository

```bash
git clone https://github.com/thryveframework/thryve.git
cd thryve
```

### Verify Installation

```bash
# Build the project
cargo build

# Run tests (on one thread: some tests share files and global state)
cargo test --workspace -- --test-threads=1

# Check code style
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

---

## How to Contribute

### Types of Contributions

| Type | Description |
|------|-------------|
| 🐛 **Bug Reports** | Unexpected behavior, crashes, errors |
| 💡 **Feature Requests** | New functionality suggestions |
| 🔧 **Code Contributions** | Bug fixes, features, improvements |
| 📝 **Documentation** | Clarifications, examples, translations |
| 🎨 **Refactoring** | Code quality improvements |

### Finding Issues to Work On

1. Check the [Issues](https://github.com/thryveframework/thryve/issues) tab
2. Look for issues labeled `good first issue` or `help wanted`
3. Comment on issues you'd like to work on

---

## Development Workflow

### 1. Fork the Repository

Click the "Fork" button on GitHub to create your own copy of the repository.

### 2. Create a Branch

```bash
# Navigate to the project
cd Thryve

# Create a descriptive branch name
git checkout -b fix/issue-123
# or
git checkout -b feature/new-feature-name
```

**Branch naming conventions:**
- `fix/` - Bug fixes
- `feature/` - New features
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test additions

### 3. Make Your Changes

Follow the code style guidelines below.

If you change the C API (`crates/thryve_ffi`), build it and copy the generated header,
`crates/thryve_ffi/out/thryve.h`, over the committed one, `crates/thryve_ffi/thryve.h`. A test
fails while the two differ.

### 4. Test Your Changes

```bash
# Run all tests
cargo test --workspace -- --test-threads=1

# Run the tests whose names contain a word
cargo test -p thryve_tests cancel -- --test-threads=1

# Show what the tests print
cargo test --workspace -- --test-threads=1 --nocapture
```

### 5. Format and Lint

```bash
# Auto-format code
cargo fmt --all

# Run clippy linter
cargo clippy --workspace --all-targets -- -D warnings

# Verify everything passes, as CI does
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --test-threads=1
```

### 6. Commit Your Changes

```bash
# Add changes
git add .

# Commit with descriptive message
git commit -m "fix: resolve issue with task priority aging"
```

**Commit message conventions:**
- Use present tense ("add" not "added")
- Be concise but descriptive
- Reference issues when applicable (`Closes #123`)

### 7. Push and Create Pull Request

```bash
# Push to your fork
git push origin fix/issue-123
```

Then create a Pull Request on GitHub.

---

## Code Style

### Rust Style Guide

Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/):

```rust
/// Good: Clear, descriptive names
pub struct TaskQueue {
    priority: u8,
    created_at: DateTime<Utc>,
}

/// Good: Documentation for public items
pub fn create_task(name: &str, priority: u8) -> Task {
    // Implementation
}
```

### Naming Conventions

| Element | Convention | Example |
|---------|------------|--------|
| Types/Structs | PascalCase | `TaskQueue`, `PluginManager` |
| Functions/Methods | snake_case | `create_task`, `handle_result` |
| Constants | SCREAMING_SNAKE_CASE | `MAX_PRIORITY`, `DEFAULT_TIMEOUT` |
| Modules | snake_case | `task_queue`, `plugin_manager` |

### Documentation

Document all public items: what the item does, then `# Arguments`, `# Errors` and `# Example` where they apply:

```rust
/// Creates a task that adds up `numbers` when a worker runs it.
///
/// # Arguments
///
/// * `numbers` - The numbers to add up
/// * `priority` - Task priority (higher = more urgent, up to `tasks.max_priority`)
///
/// # Errors
///
/// Returns a `NamingError` if the task's name isn't valid.
///
/// # Example
///
/// ```
/// let task = sum_task(vec![1, 2, 3], 2)?;
/// ```
pub fn sum_task(numbers: Vec<u64>, priority: u8) -> Result<Task, NamingError> {
    // Implementation
}
```

Inside a function with several cases (branches, loops, early returns), start each block with a brief comment that says what that block handles and why, in a line or two, so a reader can follow the function without working out every condition. Simple functions don't need them.

Comments and docs describe the code as it is. Don't refer to issue numbers, milestones or design notes a reader can't see; say what the code does and why.

---

## Testing

### Writing Tests

Tests live in their own crate, `thryve_tests`, in the [`tests/`](tests/) folder, not next to the code:

- `tests/src/tests_utils`, `tests_core`, `tests_observers`, `tests_run` and `tests_ffi` hold unit tests, grouped by the crate they cover;
- `tests/src/integration` holds end-to-end tests;
- the other files each test one feature through a running engine, such as typed tasks, joins, cancelling or fan-out.

Name a test after what it shows, as a sentence:

```rust
use thryve_utils::packets::task::Task;
use thryve_utils::res::TaskResult;

#[test]
fn a_task_with_an_empty_name_is_refused() {
    let task = Task::new("", 1, || TaskResult::new("", String::new(), ""));

    assert!(task.is_err());
}
```

### Running Tests

```bash
# All tests, on one thread, as CI runs them
cargo test --workspace -- --test-threads=1

# One test, by name
cargo test -p thryve_tests a_task_with_an_empty_name_is_refused -- --test-threads=1

# With output
cargo test --workspace -- --test-threads=1 --nocapture

# Coverage (optional, with cargo-llvm-cov installed)
cargo llvm-cov --workspace -- --test-threads=1
```

---

## Submitting Changes

### Pull Request Guidelines

1. **Title**: Clear and descriptive
   - ✅ `fix: resolve memory leak in observer manager`
   - ❌ `fixes stuff`

2. **Description**: Explain what and why
   - What problem does this solve?
   - How does it work?
   - Are there breaking changes?

3. **References**: Link related issues
   - `Closes #123` or `Related to #456`

4. **Tests**: Include tests for new features

5. **Documentation**: Update docs if needed

### PR Template

```markdown
## Description

Brief description of changes.

## Type of Change

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing

- [ ] Added/updated tests
- [ ] All tests pass

## Checklist

- [ ] Code follows style guidelines
- [ ] Self-reviewed changes
- [ ] Updated documentation
- [ ] Commit messages are descriptive
```

---

## CLA (Contributor License Agreement)

### What is the CLA?

The Contributor License Agreement (CLA) is a legal agreement that:

1. **Confirms ownership**: You confirm you own the rights to your contribution
2. **Grants license**: You grant the project maintainer rights to use your contribution
3. **Enables dual-licensing**: Allows contributions to be included in both the AGPL-3.0-or-later and Commercial license editions of Thryve

The full text is in [`CLA-INDIVIDUAL.md`](CLA-INDIVIDUAL.md) — read it before signing.

### Why Do We Need It?

Thryve uses a **dual-licensing model** (AGPL-3.0-or-later + Commercial, see
[`COMMERCIAL.md`](COMMERCIAL.md)). The CLA ensures:

- ✅ Contributors retain copyright ownership
- ✅ The project can include contributions in both license editions
- ✅ Commercial licensing is legally viable
- ✅ Future-proofing for the project

### How to Sign (Individual Contributors)

Almost all contributors should sign [`CLA-INDIVIDUAL.md`](CLA-INDIVIDUAL.md).
It is verified automatically the first time you open a Pull Request:

1. Submit your Pull Request.
2. The CLA Assistant bot will comment with a link to the agreement and
   instructions.
3. Reply on the Pull Request with the comment text the bot asks for to
   record your signature.
4. Your PR will be marked as CLA-signed. You only need to do this once —
   it covers all your future contributions.

### Contributing on Behalf of a Company

If your employer owns the rights to your contribution and you want your
organization — not you personally — to be the one granting the license,
see [`CLA-ENTITY.md`](CLA-ENTITY.md). This one is **not** bot-automated:
reach out via [thryve-framework.com](https://thryve-framework.com) first to
arrange signing it. (Alternatively, `CLA-INDIVIDUAL.md` §3(c) lets an
individual contributor's employer simply approve their signing it instead —
the Entity agreement is only needed if the company wants to be the
contracting party itself.)

### Questions About CLA?

- The CLA does **not** transfer copyright ownership to the project
- You retain ownership of your contributions
- The CLA only grants the licensing rights described in
  [`CLA-INDIVIDUAL.md`](CLA-INDIVIDUAL.md) / [`CLA-ENTITY.md`](CLA-ENTITY.md)
- Contact the maintainer via [thryve-framework.com](https://thryve-framework.com) for questions

---

## Questions or Help?

- 📝 **General questions**: Open an issue
- 🏗️ **Architecture questions**: Contact maintainers
- 💬 **Discussion**: Create a discussion thread

---

## Thank You!

Your contributions make Thryve better for everyone. We appreciate your time and effort! 🚀

---

*Based on contributing guidelines from various open-source projects, adapted for Thryve's dual-licensing model.*
