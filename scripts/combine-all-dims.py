import glob
import json

all_classifications = {}

for filename in glob.glob("downloaded/classifications-*.json"):
    with open (filename, 'r') as f:
        data = json.load(f)
    for item in data["items"]:
        all_classifications[item["id"]] = item

with open('generated/all-classifications.json', 'w') as f:
    json.dump(all_classifications, f)
