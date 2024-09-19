import subprocess
import os
from pathlib import Path

# Define paths
mafft_path = r"C:\Users\mrnaj\PycharmProjects\MAFFT\mafft-7.526-win64-signed\mafft-win\mafft.bat"
iqtree_path = r"C:\Users\mrnaj\PycharmProjects\iqtree-2.2.0-Windows\iqtree2.exe"
input_dir = r"C:\Users\mrnaj\PycharmProjects\NCBI_project_2\concatenate\result"
output_dir = r"C:\Users\mrnaj\PycharmProjects\NCBI_project_2\concatenate\phylogeny"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Ensure the current working directory is set correctly
os.chdir(r"C:\Users\mrnaj\PycharmProjects\MAFFT\mafft-7.526-win64-signed\mafft-win")

def run_mafft(input_file_path, output_file_path):
    command = [mafft_path, '--auto', input_file_path]
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        with open(output_file_path, 'w') as aligned_file:
            aligned_file.write(result.stdout)
        if result.stderr:
            print(f"MAFFT error for {input_file_path}: {result.stderr}")
        else:
            print(f"MAFFT successfully aligned {input_file_path}.")
        if os.path.getsize(output_file_path) == 0:
            print(f"Warning: {output_file_path} is empty. Check if the input file has valid sequences.")
            with open(input_file_path, 'r') as file:
                print(file.read())
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running MAFFT on {input_file_path}:")
        print("Return code:", e.returncode)
        print("Error output:", e.stderr)
    except Exception as ex:
        print(f"An unexpected error occurred with file {input_file_path}: {ex}")

def rename_sequences_in_fasta(fasta_path):
    """Rename sequences in a FASTA file to simple names like seq1, seq2, etc."""
    renamed_path = fasta_path.replace('.fasta', '_renamed.fasta')
    with open(fasta_path, 'r') as original, open(renamed_path, 'w') as renamed:
        seq_count = 1
        for line in original:
            if line.startswith('>'):
                line = f">seq{seq_count}\n"
                seq_count += 1
            renamed.write(line)
    return renamed_path

def clean_alignment(aligned_file_path):
    """Use trimAl or other alignment tools to clean the alignment file."""
    cleaned_file_path = aligned_file_path.replace('.fasta', '_cleaned.fasta')
    command = ['trimal', '-in', aligned_file_path, '-out', cleaned_file_path, '-automated1']
    try:
        subprocess.run(command, check=True)
        print(f"Alignment cleaned: {cleaned_file_path}")
        return cleaned_file_path
    except FileNotFoundError:
        print("trimAl is not installed or not found in PATH. Please install trimAl or clean the alignment manually.")
        return aligned_file_path
    except Exception as ex:
        print(f"An error occurred while cleaning the alignment: {ex}")
        return aligned_file_path

def run_iqtree(aligned_file_path, sequence_type="DNA"):
    renamed_file_path = rename_sequences_in_fasta(aligned_file_path)  # Rename sequences
    cleaned_file_path = clean_alignment(renamed_file_path)  # Clean the alignment
    command = [iqtree_path, '-s', cleaned_file_path, '-st', sequence_type, '-nt', 'AUTO']
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        if result.stderr:
            print(f"IQ-TREE error for {aligned_file_path}: {result.stderr}")
        else:
            print(f"IQ-TREE successfully constructed the tree for {aligned_file_path}.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running IQ-TREE on {aligned_file_path}:")
        print("Return code:", e.returncode)
        print("Error output:", e.stderr)
    except Exception as ex:
        print(f"An unexpected error occurred with file {aligned_file_path}: {ex}")

# Process each .fasta file in the input directory
for input_file in Path(input_dir).glob("*.fasta"):
    input_file_path = str(input_file)
    aligned_file_path = os.path.join(output_dir, f"aligned_{input_file.stem}.fasta")
    run_mafft(input_file_path, aligned_file_path)  # Run MAFFT for alignment
    run_iqtree(aligned_file_path, sequence_type="DNA")  # Run IQ-TREE on the aligned file

print("Phylogenetic tree construction is complete.")
