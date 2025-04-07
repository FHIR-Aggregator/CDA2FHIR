import json
import argparse
# R5 -> R4 changes


def build_bodystructure_dict(filename):
    bs_dict = {}
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            resource = json.loads(line)
            bs_id = resource.get('id')
            if not bs_id:
                continue

            if 'includedStructure' in resource:
                included = resource['includedStructure']
                if isinstance(included, list) and included:
                    # Assume the first element holds the structure information
                    structure = included[0].get('structure', {})
                    coding = structure.get('coding')
                    if coding:
                        bs_dict[bs_id] = coding
                    else:
                        print(f"Warning: BodyStructure {bs_id} found but missing 'structure.coding' in includedStructure.")
                else:
                    print(f"Warning: BodyStructure {bs_id} has an empty or invalid 'includedStructure'.")
            else:
                print(f"Warning: BodyStructure {bs_id} does not have an 'includedStructure' field.")
    return bs_dict

def process_specimens(specimen_file, bodystructure_dict, output_file):
    """
    Process each Specimen resource in the NDJSON file. If a specimen has a collection.bodySite
    that references a BodyStructure, look up the corresponding coding from the dictionary and
    update the specimen's bodySite to use a CodeableConcept.
    """
    with open(specimen_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            line = line.strip()
            if not line:
                continue
            
            specimen = json.loads(line)
            if 'collection' in specimen and 'bodySite' in specimen['collection']:
                bs_field = specimen['collection']['bodySite']
                reference_value = None

                # Determine if bs_field contains a reference string
                if isinstance(bs_field, dict):
                    if 'reference' in bs_field:
                        if isinstance(bs_field['reference'], str):
                            reference_value = bs_field['reference']
                        elif isinstance(bs_field['reference'], dict) and 'reference' in bs_field['reference']:
                            reference_value = bs_field['reference']['reference']
                
                if reference_value and reference_value.startswith("BodyStructure/"):
                    bs_id = reference_value.split("BodyStructure/")[1]
                    if bs_id in bodystructure_dict:
                        coding = bodystructure_dict[bs_id]
                        specimen['collection']['bodySite'] = {"coding": coding}
                    else:
                        print(f"Warning: Specimen references BodyStructure {bs_id} but it was not found in the BodyStructure file.")
            
            outfile.write(json.dumps(specimen) + "\n")

def main():
    parser = argparse.ArgumentParser(
        description="Update Specimen NDJSON: Convert collection.bodySite from a valueReference to a CodeableConcept using BodyStructure NDJSON."
    )
    parser.add_argument("--specimen", required=True, help="Path to the Specimen NDJSON file.")
    parser.add_argument("--bodystructure", required=True, help="Path to the BodyStructure NDJSON file.")
    parser.add_argument("--output", required=True, help="Path for the output NDJSON file with updated specimens.")
    args = parser.parse_args()

    # dictionary of BodyStructure id -> coding (from includedStructure)
    bodystructure_dict = build_bodystructure_dict(args.bodystructure)
    # process Specimen file and update the collection.bodySite field to be a codeable concept
    process_specimens(args.specimen, bodystructure_dict, args.output)

if __name__ == "__main__":
    main()

