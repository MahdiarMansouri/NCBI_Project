import subprocess
import os

class BLAST:
    def __init__(self, WGS, gene):
        self.WGS = WGS
        self.gene = gene

    def blast(self):
        # Create BLAST database
        result = subprocess.run(["makeblastdb", "-version"], capture_output=True, text=True)
        print("makeblastdb version output:", result.stdout)
        if result.stderr:
            print("makeblastdb version error:", result.stderr)

        result = subprocess.run(["makeblastdb", "-in", self.WGS, "-dbtype", "nucl", "-out", "WGS"], capture_output=True,
                                text=True)
        print("makeblastdb output:", result.stdout)
        if result.stderr:
            print("makeblastdb error:", result.stderr)

        # Perform BLAST search
        result = subprocess.run(
            ["blastn", "-query", f"{self.gene}", "-db", "WGS", "-out", f"{self.gene}.csv", "-outfmt",
             "10 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore qlen slen sstrand qframe sframe qseq sseq"],
            capture_output=True, text=True
        )
        print("blastn output:", result.stdout)
        if result.stderr:
            print("blastn error:", result.stderr)
