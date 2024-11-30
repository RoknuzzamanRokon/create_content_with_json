import json

file1_path = "D:/hotels_content_to_create_json_file/TBO/tracking_file_for_tbo_content_create.txt"
file2_path = "D:/hotels_content_to_create_json_file/TBO/tbo_done_content_creations_tracking_file.txt"
output = "final_tbo_content_file_path.txt"

with open(file1_path, 'r') as file1:
    file1_data = {line.strip() for line in file1}

with open(file2_path, 'r') as file2:
    file2_data = {line.strip().rstrip(',') for line in file2}

unique_to_file2 = file1_data - file2_data

with open(output, 'w') as file3:
    file3.write('\n'.join(unique_to_file2))

print(f"Unique items writen to {output}")

