import subprocess
import os

class BLAST:
    def __init__(self, WGS, gene):
        self.WGS = WGS
        self.gene_name = gene.name
        self.gene_path = gene.file_path

    def blast(self):
        # Create BLAST database
        result = subprocess.run(["makeblastdb", "-in", self.WGS, "-dbtype", "nucl", "-out", "wgs\WGS"], capture_output=True,
                                text=True)
        print("makeblastdb output:", result.stdout)
        if result.stderr:
            print("makeblastdb error:", result.stderr)

        # Perform BLAST search
        result = subprocess.run(
            ["blastn", "-query", f"{self.gene_path}", "-db", "wgs\WGS", "-out", f"{self.gene_name}.csv", "-outfmt",
             "10 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore qlen slen sstrand qframe sframe qseq sseq"],
            capture_output=True, text=True
        )
        print("blastn output:", result.stdout)
        if result.stderr:
            print("blastn error:", result.stderr)