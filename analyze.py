import json
import os

files = os.listdir(f"results")
files = [os.path.join("results", file) for file in files]

items = {}

for file in files:
    with open(file) as f:
        contents = f.readlines()
        for line in contents:
            entry = line.strip().split(":")
            if len(entry) != 5:
                continue
            _, path, func, arg, type_ = entry
            key = f"{path}:{func}"
            if key not in items:
                items[key] = {}
            if arg not in items[key]:
                items[key][arg] = {}
            if type_ not in items[key][arg]:
                items[key][arg][type_] = 0
            items[key][arg][type_] += 1


with open("analysis.json", "w") as f:
    f.write(json.dumps(items, indent=4, sort_keys=True))
