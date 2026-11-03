# Publishing Guide: Submitting Sphuling to PyPI

This guide provides step-by-step instructions for building, validating, and publishing `sphuling` to the **Python Package Index (PyPI)** so anyone in the world can install it with `pip install sphuling` or `uv add sphuling`.

---

## 1. Prerequisites

1. **PyPI Account:** Register an account at [pypi.org](https://pypi.org).
2. **TestPyPI Account (Recommended for testing):** Register at [test.pypi.org](https://test.pypi.org).
3. **API Token:**
   - Log in to PyPI $\rightarrow$ Account Settings $\rightarrow$ **API Tokens**.
   - Create a new token with name `sphuling-release`.
   - Copy the token string (starts with `pypi-...`).

---

## 2. Step 1: Install Build & Release Tools

Ensure you have `build` and `twine` installed in your virtual environment:

```bash
# Using pip
pip install --upgrade build twine

# Or using uv
uv pip install build twine
```

---

## 3. Step 2: Validate `pyproject.toml` Metadata

Before building, verify that `pyproject.toml` contains all necessary metadata:
- Name: `sphuling`
- Version: `0.1.0` (increment on each release: `0.1.1`, `0.2.0`, etc.)
- License: `Apache-2.0`
- Dependencies: `datafusion`, `duckdb`, `pyarrow`, `sqlglot`, `pandas`, `cloudpickle`
- Package discovery: `src/pysphuling` and `src/sphuling`

---

## 4. Step 3: Build the Distribution Artifacts

Run the Python build tool from the project root:

```bash
# Clean any previous artifacts
rm -rf dist/ build/ *.egg-info

# Build source distribution (.tar.gz) and binary wheel (.whl)
python -m build
```

Verify that the `dist/` directory contains:
```text
dist/
├── sphuling-0.1.0-py3-none-any.whl
└── sphuling-0.1.0.tar.gz
```

---

## 5. Step 4: Validate Package Metadata with `twine check`

Check that your package description and metadata meet PyPI standards:

```bash
twine check dist/*
```

Expected output:
```text
Checking dist/sphuling-0.1.0-py3-none-any.whl: PASSED
Checking dist/sphuling-0.1.0.tar.gz: PASSED
```

---

## 6. Step 5: Test Release on TestPyPI (Recommended)

Upload your package to TestPyPI first to verify without affecting the official index:

```bash
twine upload --repository testpypi dist/*
```
- **Username:** `__token__`
- **Password:** `<Your-TestPyPI-API-Token>`

### Test the Installation:
In a separate clean terminal or environment:
```bash
# Install from TestPyPI with dependencies from official PyPI
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sphuling

# Test import
python -c "from pysphuling.sql import SphulingSession; print(SphulingSession.builder.getOrCreate().range(3).collect())"
```

---

## 7. Step 6: Publish to Production PyPI

Once verified on TestPyPI, publish to official PyPI:

### Option A: Using `twine`
```bash
twine upload dist/*
```
- **Username:** `__token__`
- **Password:** `<Your-Production-PyPI-API-Token>`

### Option B: Using `uv publish`
```bash
uv publish --token <Your-Production-PyPI-API-Token>
```

Your package will be immediately live at:
👉 **`https://pypi.org/project/sphuling/`**

---

## 8. Step 7: How Users Install and Use It

Once published, any user or AI agent anywhere can install Sphuling in seconds:

### Standard installation:
```bash
pip install sphuling
```

### Ultra-fast installation with `uv`:
```bash
uv add sphuling
```

### Using in their Python code:
```python
import pysphuling as pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema=["id", "name"])
df.show()
```

---

## 9. Automated CI/CD Publishing (GitHub Actions)

To automatically publish new releases when you push a Git tag, create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write # Required for Trusted Publishing (OIDC)

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install build tools
        run: pip install build

      - name: Build sdist and wheel
        run: python -m build

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
```
*(With PyPI Trusted Publishing, you don't even need hardcoded tokens—GitHub Actions authenticates via secure OpenID Connect!)*

