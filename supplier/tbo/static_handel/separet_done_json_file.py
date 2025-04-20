import os

def read_existing_files(file_path):
    existing_files = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            existing_files = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        print(f"File '{file_path}' not found. Starting with an empty list.")
    return existing_files

def write_files_to_txt(file_path, files):
    with open(file_path, 'w', encoding='utf-8') as f:
        for file_name in files:
            f.write(file_name + '\n')

def main():
    directory_path = r'D:\content_for_hotel_json\cdn_row_collection\tbo'
    output_file_path = 'tbo_done_json.txt'

    # Read existing file names to avoid duplicates
    existing_files = read_existing_files(output_file_path)

    # Get all JSON file names in the directory and strip the .json extension
    json_files = [os.path.splitext(f)[0] for f in os.listdir(directory_path) if f.endswith('.json')]

    # Filter out files that are already in the existing list
    new_files = [file for file in json_files if file not in existing_files]

    # Combine existing files with new files
    all_files = existing_files.union(new_files)

    # Write the combined list back to the file
    write_files_to_txt(output_file_path, all_files)

    print(f"Processing complete. Added {len(new_files)} new JSON file names to {output_file_path}.")

if __name__ == "__main__":
    main()
