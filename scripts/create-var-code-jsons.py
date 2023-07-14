import json

def create_json(input_filename, output_filename):
    with open(input_filename, 'r') as f:
        datasets = [line.strip() for line in f]
    with open(output_filename, 'w') as f:
        json.dump(datasets, f, indent=4)


create_json('input-txt-files/input-classifications.txt', 'generated/input-classifications.json')
create_json('input-txt-files/output-classifications.txt', 'generated/output-classifications.json')
