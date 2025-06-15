import json

data = {
  "id": 100,
  "name": "Bozon Incorporated",
  "employees": [{"id": "1", "name": "Александр"}]
}

with open("json_data.json", "w", encoding="UTF-8") as json_file:
    json.dump(data, json_file)