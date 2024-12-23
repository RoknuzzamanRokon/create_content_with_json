import json

file1_path = "D:/Rokon/hotels_content_to_create_json_file/GRNConnect/tracking_file_for_GRNConnect_content_create.txt"
file2_path = "D:/Rokon/hotels_content_to_create_json_file/GRNConnect/GRNConnect_done_content_creations_tracking_file.txt"
output = "final_GRNConnect_content_file_path.txt"

with open(file1_path, 'r') as file1:
    file1_data = {line.strip() for line in file1}

with open(file2_path, 'r') as file2:
    file2_data = {line.strip().rstrip(',') for line in file2}

unique_to_file2 = [item for item in file1_data if item not in file2_data]

with open(output, 'w') as file3:
    file3.write('\n'.join(unique_to_file2))

print(f"Unique items write to--------- {output}")

