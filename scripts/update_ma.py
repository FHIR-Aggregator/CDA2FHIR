import json
import argparse
# R5 -> R4 changes

def process_medication_administration(input_file, output_file, effective_datetime):
    # typo in R5 field
    fields_to_remove = ["occurrenceTiming", "occurenceTiming"]
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            line = line.strip()
            if not line:
                continue
            
            record = json.loads(line)
            
            for field in fields_to_remove:
                record.pop(field, None)
            
            record['effectiveDateTime'] = effective_datetime
            
            outfile.write(json.dumps(record) + "\n")

def main():
    parser = argparse.ArgumentParser(
        description="Update MedicationAdministration NDJSON: Remove occurrenceTiming (and variant) and add effectiveDateTime field."
    )
    parser.add_argument('--input', required=True, help="Path to the input MedicationAdministration NDJSON file.")
    parser.add_argument('--output', required=True, help="Path for the output NDJSON file with updated resources.")
    parser.add_argument('--datetime', default="2025-01-01T00:00:00+00:00",
                        help="Arbitrary effectiveDateTime value in ISO 8601 format with time zone (default: 2025-01-01T00:00:00+00:00)")
    args = parser.parse_args()
    
    process_medication_administration(args.input, args.output, args.datetime)

if __name__ == "__main__":
    main()

