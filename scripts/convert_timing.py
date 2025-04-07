import json
from datetime import datetime, timezone

def process_records(input_file, output_file, arbitrary_start):

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)

                if "occurenceTiming" in record:
                    record.pop("occurenceTiming")
                    record["occurenceDateTime"] = arbitrary_start

                outfile.write(json.dumps(record) + "\n")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

if __name__ == "__main__":
    input_file = "./MedicationAdministration.ndjson"
    output_file = "./MedicationAdministration_updated.ndjson"

    # TODO: subtract bounds and make effectivePeriod for R4
    arbitrary_start = "2025-01-01T00:00:00Z"

    process_records(input_file, output_file, arbitrary_start)
