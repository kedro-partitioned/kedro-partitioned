# Kedro Partitioned CLI

Welcome to Kedro partitioned development commands. In order to use it you first need
to install [mask](https://github.com/jacobdeichert/mask). Once it is installed you can
run `mask install` to install development tools.

The list of available commands can be found using `mask help`.

## install

> Install kedro partitioned with its dependencies for development

```bash
python -m venv .venv
source .venv/bin/activate
pip install uv
find ./requirements -name "*.txt" | xargs printf -- '-r %s\n' | xargs uv pip install
uv pip install -e . --no-deps
pre-commit install
```

## test

> Check if package is working properly

### pytest

> Run pytest tests

```bash
source .venv/bin/activate
python -m pytest
```

### requirements

> Check if requirements are resolvable

```bash
source .venv/bin/activate
find ./requirements -name "*.txt" | xargs uv pip compile
```

## build

> Build package wheel

```bash
source .venv/bin/activate
pip wheel . -w dist --no-deps
```

## publish (api_token)

> Publish package to pypi

**OPTIONS**

- prod
  - flags: -p --prod
  - desc: Whether to publish to production or test pypi

```bash
source .venv/bin/activate
repo="https://test.pypi.org/legacy/"
if [[ "$prod" == "true"  ]]; then
    repo="https://upload.pypi.org/legacy/"
fi
twine upload dist/* \
    --non-interactive \
    --repository-url $repo \
    --username __token__ --password $api_token
```

## lint

> Check if code is following the style guide

**OPTIONS**

- all
  - flags: -a --all
  - desc: Whether to run linters on all files or only staged files

```bash
source .venv/bin/activate
if [[ "$all" == "true" ]]; then
    pre-commit run --all-files
else
    pre-commit
fi
```

## ci

> Run all CI checks

```bash
python -m venv .venv
source .venv/bin/activate
pip install uv && \
echo "Checking requirements..." && mask test requirements && echo "OK" && \
cat requirements/requirements-test.txt | grep pre-commit | xargs uv pip install
echo "Checking linting..." && mask lint && echo "OK" && \
echo "Installing deps..." && \
uv pip install -r requirements/requirements.txt -r requirements/requirements-test.txt && \
echo "Running tests..." && mask test pytest && echo "OK"
```

## cd (api_token)

> Run deployment routines

```bash
mask build && \
cat requirements/requirements-dev.txt | grep twine | xargs pip install && \
mask publish $api_token --prod
```
