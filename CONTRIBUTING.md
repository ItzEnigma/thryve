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

- **Rust 1.85+** - [Install Rust](https://rustup.rs/)
- **Git** - Version control
- **GitHub account** - For submitting pull requests

### Clone the Repository

```bash
git clone https://github.com/JoJoAbdellatif/Thryve.git
cd Thryve
```

### Verify Installation

```bash
# Build the project
cargo build

# Run tests
cargo test

# Check code style
cargo fmt --check
cargo clippy
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

1. Check the [Issues](https://github.com/JoJoAbdellatif/Thryve/issues) tab
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

### 4. Test Your Changes

```bash
# Run all tests
cargo test

# Run tests for specific package
cargo test -p thryve_core

# Run with verbose output
cargo test -- --nocapture
```

### 5. Format and Lint

```bash
# Auto-format code
cargo fmt

# Run clippy linter
cargo clippy -- -D warnings

# Verify everything passes
cargo fmt --check
cargo clippy -- -D warnings
cargo test
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

Document all public items:

```rust
/// Creates a new task with the specified name and priority.
///
/// # Arguments
///
/// * `name` - Human-readable task identifier
/// * `priority` - Task priority (0-14, higher = more urgent)
///
/// # Returns
///
/// A new `Task` instance ready for execution.
///
/// # Example
///
/// ```
/// let task = Task::new("MyTask", 5);
/// ```
pub fn new(name: &str, priority: u8) -> Self {
    // Implementation
}
```

---

## Testing

### Writing Tests

Add tests in the same file or in `tests/` directory:

```rust
#[cfg(test)]
pmod tests {
    use super::*;

    #[test]
    fn test_task_creation() {
        let task = Task::new("Test", 5);
        assert_eq!(task.name, "Test");
        assert_eq!(task.priority, 5);
    }
}
```

### Running Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_task_creation

# With output
cargo test -- --nocapture

# Coverage (if configured)
cargo tarpaulin
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
