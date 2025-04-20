def read_file_to_set(file_path):
    file_set = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            file_set = {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    return file_set

def write_set_to_file(file_path, data_set):
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in sorted(data_set):  # Sorting for better readability
            f.write(item + '\n')

def main():
    file_a_path = 'tbo_done_json.txt'
    file_b_path = 'tbohotel_hotel_id_list.txt'
    output_file_path = 'cannot_done.txt'

    # Read the contents of both files into sets
    set_a = read_file_to_set(file_a_path)
    set_b = read_file_to_set(file_b_path)

    # Compute the difference
    difference =  set_b - set_a

    # Write the difference to the output file
    write_set_to_file(output_file_path, difference)

    print(f"Processing complete. The difference has been written to {output_file_path}.")

if __name__ == "__main__":
    main()
