def split_file(input_file, output_prefix, lines_per_file):
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            line_count = 0
            file_index = 1
            output_file = None
            
            for line in file:
                if line_count % lines_per_file == 0:
                    if output_file:
                        output_file.close()
                    output_filename = f"{output_prefix}{file_index:02d}.txt"
                    output_file = open(output_filename, 'w', encoding='utf-8')
                    file_index += 1

                output_file.write(line)
                line_count += 1

            if output_file:
                output_file.close()

        print(f"File split completed! {file_index - 1} files created.")
    except Exception as e:
        print(f"An error occurred: {e}")



input_file = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/restel_tracking_file.txt"
output_prefix = "text"
lines_per_file = 28000

split_file(input_file, output_prefix, lines_per_file)