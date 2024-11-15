#!/bin/bash

input_file=$1
output_file=$2
pattern=$3

# run jq -c to compress instead of pretty print the json file 
# cat mutation.json | jq -c . >> mutation2.json
# grep --invert-match "$pattern" "$input_file" > "$output_file"

grep "$pattern" "$input_file" > "$output_file"
echo "Filtered lines containing '$pattern' are stored in '$output_file'."

