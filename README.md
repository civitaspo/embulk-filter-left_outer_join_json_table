# Left Outer Join Json Table filter plugin for Embulk

## JSON Table means...

a json array with this format:

```
[
  {
    "id": 0,
    "name": "civitaspo"
  },
  {
    "id": 2,
    "name": "moriogai"
  },
  {
    "id": 5,
    "name": "natsume.soseki"
  }
]
```

## Overview

* **Plugin type**: filter

## Configuration

- **base_column**: a column name of data embulk loaded (hash, required)
  - **name**: name of the column
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp
- **counter_column**: a column name of json table (string, default: `{name: id, type: long}`)
  - **name**: name of the column
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp
- **joined_keys_prefix**: prefix added to joined json table keys (string, default: `"_joined_by_embulk_"`)
- **json_file_path**: path of json file (string, required)
- **json_columns**: required columns of json table (array of hash, required)
  - **name**: name of the column
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp

---
**type of the column**

|name|description|
|:---|:---|
|boolean|true or false|
|long|64-bit signed integers|
|timestamp|Date and time with nano-seconds precision|
|double|64-bit floating point numbers|
|string|Strings|

## Example

```yaml
filters:
  - type: left_outer_join_json_table
    base_column: {name: name_id, type: long}
    counter_column: {name: id, type: long}
    joined_keys_prefix: _joined_by_embulk_
    json_file_path: master.json
    json_columns:
      - {name: id, type: long}
      - {name: name, type: string}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
