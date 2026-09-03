# Security Policy

## Supported Versions

Thryve is pre-1.0 software. Only the latest commit on `main` is supported;
there are no maintained release branches yet.

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities,
especially anything involving the FFI layer's memory-safety boundary
(`thryve_ffi`) or the Windows service host (`thryve_run`).

Instead, use GitHub's private vulnerability reporting:

1. Go to the **Security** tab of this repository.
2. Click **Report a vulnerability**.
3. Describe the issue, affected component(s), and reproduction steps if
   available.

This opens a private advisory visible only to the maintainer and you, so the
issue can be triaged and fixed before public disclosure.

You should expect an initial response within a reasonable timeframe. Given
this is currently a single-maintainer project, please be patient — there is
no formal SLA yet.

## Scope

Given Thryve's architecture, the highest-value reports are:

- Memory-safety issues at the C FFI boundary (`thryve_ffi`) — use-after-free,
  double-free, buffer overruns, unsound pointer handling.
- Privilege or isolation issues in the Windows service host (`thryve_run`).
- Panics/DoS reachable from untrusted task, plugin, or observer input.

Issues in third-party dependencies should generally be reported upstream, but
feel free to report here too if you're unsure where it belongs.
