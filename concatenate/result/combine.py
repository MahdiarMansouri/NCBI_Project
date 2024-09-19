import os

# Define the path to the directory containing the FASTA files
directory_path = r"C:\Users\mrnaj\PycharmProjects\NCBI_project_2\concatenate\result"

# Define the output file path
output_file = os.path.join(directory_path, "combined_fasta.fasta")

# Initialize a list to store file contents
fasta_contents = []

# Loop through all files in the directory
for filename in os.listdir(directory_path):
    # Check if the file is a FASTA file (assuming they have .fasta extension)
    if filename.endswith(".fasta"):
        file_path = os.path.join(directory_path, filename)
        with open(file_path, 'r') as file:
            # Add a header with the filename to maintain meaningful names
            # Ensure it's unique by keeping filename without additional counters
            fasta_contents.append(f">{filename}")
            # Read and append the sequences, ensuring no empty lines or malformed entries
            sequence_lines = [line.strip() for line in file if line.strip() and not line.startswith('>')]
            fasta_contents.extend(sequence_lines)

# Write the combined contents to the output file if at least 2 sequences exist
if len(fasta_contents) >= 4:  # Minimum 2 sequences with headers and sequences
    with open(output_file, 'w') as output:
        output.write('\n'.join(fasta_contents))
    print(f"FASTA files combined into {output_file}")
else:
    print("Error: A minimum of 2 sequences is required.")
