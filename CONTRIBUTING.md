# Contributing to Data Quality Framework

Thank you for your interest in contributing to the Data Quality Framework! This document provides guidelines and information for contributors.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## How to Contribute

### Reporting Bugs

Before creating a bug report, please check existing issues to avoid duplicates. When creating a bug report, include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior vs actual behavior
- Your environment (Python version, Spark version, OS)
- Relevant logs or error messages
- Sample configuration if applicable

### Suggesting Features

Feature requests are welcome! Please provide:

- A clear description of the feature
- The problem it solves or use case it enables
- Any implementation ideas you have
- Whether you'd be willing to help implement it

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Install development dependencies**:
   ```bash
   poetry install --with dev -E spark -E deequ
   ```
3. **Make your changes** following the coding standards below
4. **Add tests** for any new functionality
5. **Run the test suite**:
   ```bash
   poetry run pytest dq/tests/ -v
   ```
6. **Run linting**:
   ```bash
   poetry run black dq/
   poetry run flake8 dq/
   ```
7. **Update documentation** if needed
8. **Submit your pull request**

## Development Setup

### Prerequisites

- Python 3.12 or 3.13
- Poetry (for dependency management)
- Java 11+ (for Spark/Deequ)
- Git

---

### macOS

```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Java 11, Python, and Poetry
brew install openjdk@11 python@3.12 poetry

# Set JAVA_HOME
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
source ~/.zshrc

# Verify
java -version   # should show 11.x
python3 --version
poetry --version
```

### Ubuntu / Debian

```bash
# Install Java 11 and Python
sudo apt update
sudo apt install -y openjdk-11-jdk python3 python3-pip python3-venv curl

# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc

# Verify
java -version
python3 --version
poetry --version
```

### Windows (WSL2 recommended)

```powershell
# 1. Install WSL2 (run in PowerShell as Administrator)
wsl --install -d Ubuntu

# 2. Inside WSL2 Ubuntu, follow the Ubuntu instructions above.
```

If you prefer native Windows (without WSL2):

```powershell
# Install Chocolatey (run in PowerShell as Administrator)
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install Java 11 and Python
choco install temurin11 python312 -y

# Install Poetry
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

# Set JAVA_HOME (adjust path if different)
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-11", "User")

# Restart your terminal, then verify
java -version
python --version
poetry --version
```

---

### Clone and Install

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/data-quality-framework.git
cd data-quality-framework

# Install all dependencies (core + dev + extras)
poetry install --with dev -E spark -E deequ -E aws

# Download Deequ JAR for testing
mkdir -p lib
curl -L -o lib/deequ-2.0.8-spark-3.5.jar \
  https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.8-spark-3.5/deequ-2.0.8-spark-3.5.jar
```

---

### VSCode Setup

Install [Visual Studio Code](https://code.visualstudio.com/) and the following extensions:

| Extension | ID | Purpose |
|-----------|----|---------|
| Python | `ms-python.python` | Python language support |
| Pylance | `ms-python.vscode-pylance` | Type checking and IntelliSense |
| Black Formatter | `ms-python.black-formatter` | Code formatting |
| Flake8 | `ms-python.flake8` | Linting |
| Python Test Explorer | `littlefoxteam.vscode-python-test-adapter` | Test discovery and runner |

Create a workspace settings file at `.vscode/settings.json`:

```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": ["dq/tests/", "-v"],
  "editor.formatOnSave": true,
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter"
  },
  "black-formatter.args": ["--line-length", "88"],
  "flake8.args": ["--max-line-length", "88", "--extend-ignore", "E203,W503"],
  "python.analysis.typeCheckingMode": "basic",
  "python.envFile": "${workspaceFolder}/.env",
  "files.exclude": {
    "**/__pycache__": true,
    "**/.pytest_cache": true,
    "spark-warehouse": true
  }
}
```

Create a `.env` file in the project root for Spark/Deequ tests:

```
SPARK_VERSION=3.5
JAVA_HOME=/path/to/java-11
```

Create a `.vscode/launch.json` for debugging:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Run DQ Framework",
      "type": "debugpy",
      "request": "launch",
      "module": "dq.cli",
      "args": ["file://examples/sample_config.conf"],
      "env": {"SPARK_VERSION": "3.5"}
    },
    {
      "name": "Run Current Test File",
      "type": "debugpy",
      "request": "launch",
      "module": "pytest",
      "args": ["${file}", "-v", "-s"],
      "env": {"SPARK_VERSION": "3.5"}
    }
  ]
}
```

---

### Running Tests

```bash
# Run all tests
poetry run pytest dq/tests/ -v

# Run specific test file
poetry run pytest dq/tests/test_framework.py -v

# Run tests by marker
poetry run pytest -m unit
poetry run pytest -m spark

# Run tests with coverage
poetry run pytest dq/tests/ --cov=dq --cov-report=html
```

## Coding Standards

### Python Style

- Follow [PEP 8](https://pep8.org/)
- Use [Black](https://black.readthedocs.io/en/stable/) for code formatting (line length: 88)
- Use type hints for function signatures
- Write docstrings for public classes and methods (Google style)

### Code Example

```python
from typing import List, Dict, Optional

def process_checks(
    checks: List[Dict],
    dataframe: "DataFrame",
    strict: bool = True
) -> List[Dict[str, Any]]:
    """Process a list of data quality checks.

    Args:
        checks: List of check configurations.
        dataframe: Spark DataFrame to validate.
        strict: If True, raise on first failure.

    Returns:
        List of check results with success status.

    Raises:
        ValidationError: If strict mode and a check fails.
    """
    results = []
    # Implementation
    return results
```

### Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in imperative mood (Add, Fix, Update, Remove)
- Keep the first line under 72 characters
- Reference issues when applicable

Examples:
```
Add support for custom constraint engines
Fix null handling in schema validation
Update documentation for CLI usage
Remove deprecated configuration options (#123)
```

## Adding a New Engine

To add a new validation engine:

1. Create a new directory under `dq/engine/`:
   ```
   dq/engine/myengine/
   ├── __init__.py
   └── myengine_engine.py
   ```

2. Implement the engine class:
   ```python
   from dq.engine.dq_engine import DQEngine

   class MyengineEngine(DQEngine):
       def apply(self, dataframe, repository=None):
           results = []
           for check in self._config.get("checks", []):
               # Implement validation logic
               results.append({
                   "check": check.get("constraint_name"),
                   "success": True,
                   "details": {}
               })
           return results
   ```

3. Add tests in `dq/tests/test_myengine.py`

4. Update documentation

## Adding Custom Constraints

Custom constraints can be added to the CustomEngine in `dq/engine/custom/custom_engine.py`. Follow the pattern of existing constraints:

1. Add the constraint class/function
2. Register it in the constraint dispatcher
3. Add tests
4. Document the constraint and its configuration options

## Questions?

If you have questions, feel free to:

- Open a GitHub issue with the "question" label
- Check existing documentation and issues

Thank you for contributing!
