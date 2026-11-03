# Installation & Setup Guide

Sphuling is distributed as a lightweight, pre-compiled Python package. It requires **Python 3.10 or higher** and has **zero system compiler requirements**—official binary wheels are provided for Windows, Linux, and macOS (x86_64 and ARM64 / Apple Silicon).

---

## 1. Quick Installation

### Using `pip`
```bash
pip install sphuling
```

### Using `uv` (Ultra-fast package manager)
```bash
uv add sphuling
# or
uv pip install sphuling
```

### Using Poetry
```bash
poetry add sphuling
```

---

## 2. Requirements & Platforms

- **Python Version:** `>= 3.10` (Python 3.10, 3.11, 3.12, 3.13 supported)
- **Supported Operating Systems:**
  - **Windows:** Windows 10, Windows 11, Windows Server (x86_64)
  - **Linux:** Ubuntu, Debian, CentOS, RHEL, Alpine, Amazon Linux 2023 (x86_64, aarch64)
  - **macOS:** macOS Monterey, Ventura, Sonoma, Sequoia (Intel & Apple Silicon M1/M2/M3/M4)
- **Compilers:** **None required.** Official ABI3 wheels contain pre-compiled Rust and C++ binaries for Apache DataFusion, DuckDB, and PyArrow.

---

## 3. Local Virtual Environment Setup (Recommended)

To keep your global Python environment pristine, install Sphuling inside an isolated virtual environment:

### Standard Python venv
```bash
# 1. Create a virtual environment
python -m venv .venv

# 2. Activate the virtual environment
# On Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# On Linux / macOS:
source .venv/bin/activate

# 3. Install sphuling
pip install --upgrade pip
pip install sphuling
```

### Fast setup with `uv`
```bash
uv venv
source .venv/bin/activate  # or .\.venv\Scripts\activate on Windows
uv pip install sphuling
```

---

## 4. Development / Editable Installation (From Source)

If you are developing Sphuling locally or contributing to the core engine:

```bash
# Clone the repository
git clone https://github.com/your-org/sphuling.git
cd sphuling

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # or .\.venv\Scripts\Activate.ps1 on Windows

# Install build dependencies
pip install setuptools wheel

# Install in editable mode
pip install -e . --no-build-isolation

# Run test suite to verify
pytest tests/
```

---

## 5. Verification Check

After installation, verify that Sphuling and its native execution engine load correctly:

```python
python -c "from pysphuling.sql import SphulingSession; spark = SphulingSession.builder.getOrCreate(); print('Sphuling successfully initialized!'); spark.range(5).show()"
```

You should see:
```text
Sphuling successfully initialized!
+----+
| id |
+----+
| 0  |
| 1  |
| 2  |
| 3  |
| 4  |
+----+
```

