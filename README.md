# Type inference and conversion

## Prerequisites

## Setup

1. Clone the repository: 
```bash
git clone https://github.com/davidRoussov/demo_server.git
cd demo_server
```

2. Setup virtual environment:

```bash
python3 -m venv ./venv
```

3. Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

## Run server

```bash
python3 ./manage.py runserver
```

## Invoke

```bash
./scripts/invoke.sh ./datasets/sample_data.csv | jq '.'
```

## Run tests

```bash
python3 ./manage.py test type_converter
```
