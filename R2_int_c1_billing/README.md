# Integration

This is the C1 Billing integration.


# Prerequisite

1. Python3.9

2. Install Poetry
```
pip3 install --user poetry
```

3. Put `Python3/bin` folder to PATH in your environment variable

# Setup
```
poetry install --no-root
```


# Build
```
sam build --template r2_int_a1_template.yaml
```

# Unit Test

Run test and coverage

```
poetry run pytest -s --show-capture=stdout -vv --cov=functions
poetry run pytest --cov tests/
```

# Explanations

1. In pytest, the environment is controlled by `pytest-env` plugin and the `pytest.ini` file
1. In pytest, the logging level is controlled by `pytest.ini`.
1. In moto, we need the `moto_server` running in order to mock s3, iam and lambda
1. All the dependencies are managed by Poetry package manager.


