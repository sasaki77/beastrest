# beastrest

This program is the RESTful API Server to publish CSS alarm status and log RDB.

## Features
- Supports only PostgreSQL backend
- Supports 2nd depth system from area
- Python 3.7 or later is required

## Installing

```bash
$ pip install -r requirements.txt
```

## Usage

```bash
export BEASTREST_SETTINGS=/path/to/settings.cfg
uwsgi --ini beastrest/uwsgi.ini
```

### settings.cfg

```
# Example configuration
DATABASE_HOST = "localhost"
DATABASE_DB = "alarm"
```

#### Supported valiables

```python
['DATABASE_HOST', 'DATABASE_USER', 'DATABASE_DB', 'DATABASE_LOGDB', 'ROOT_COMPONENT']
```

## API

- /current
- /history

### Current status

```
GET /current
```

#### Example request

```
http://localhost:5000/current?entity=MPS
```

##### Example response

```json
Status: 200 OK
```

```json
[
  {
    "group": "MPS",
    "message": "MPS issued",
    "record": "RECORD:PREFIX:MPS",
    "severity": "MAJOR",
    "status": "LOLO",
    "time": "2022-05-02T00:42:16.000Z"
  }
]
```

### Alarm history

```
GET /history
```

#### Example request

```
http://localhost:5000/history?entity=VAC&message=.*High.*&starttime=2022-05-02T00:00:00.000Z&endtime=2022-05-02T01:00:00.000Z
```

##### Example response

```json
Status: 200 OK
```

```json
[
  {
    "alarm": "Vacuum Pressure is High",
    "group": "VAC",
    "record": "RECORD:PREFIX:VAC",
    "recover": "",
    "severity": "MAJOR",
    "status": "LOLO",
    "time": "2022-05-02T00:42:16.000Z"
  },
  {
    "alarm": "Vacuum Pressure is High",
    "group": "VAC",
    "record": "RECORD:PREFIX:VAC",
    "recover": "",
    "severity": "MAJOR",
    "status": "LOLO",
    "time": "2022-05-02T00:42:16.000Z"
  }
]
```
