# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 2.1.x   | Yes                |
| 2.0.x   | No                 |
| < 2.0   | No                 |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please send an email to the project maintainers with:

1. A description of the vulnerability
2. Steps to reproduce the issue
3. Potential impact
4. Any suggested fixes (if applicable)

We will acknowledge receipt within 48 hours and aim to provide a fix or mitigation within 7 days for critical issues.

## Security Considerations

This framework processes configuration files and executes data quality checks on Spark DataFrames. When deploying, consider:

- **Configuration sources**: Configs can be loaded from S3, HTTP, and local files. Ensure access controls are in place for remote config sources.
- **Dynamic engine loading**: Engines are loaded via `importlib` from the `dq.engine` package. Engine names are validated against an alphanumeric pattern.
- **SQL execution**: Table names from configuration are validated against an identifier pattern before use in Spark SQL.
- **Lambda evaluation**: The Deequ engine supports lambda assertions in config. These are parsed via `ast.parse` and restricted to lambda expressions only.
