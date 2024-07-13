import json


class BlastResults:
    def __init__(self, query_id, subject_id, identity, alignment_length, mismathces, gap_opens, q_start, q_end,
                 s_start, s_end, evalue, bit_score, query_length, subject_length, subject_strand, query_frame,
                 subject_frame, qseq_path, sseq_path):
        self.query_id = query_id
        self.subject_id = subject_id
        self.identity = identity
        self.alignment_length = alignment_length
        self.mismathces = mismathces
        self.gap_opens = gap_opens
        self.q_start = q_start
        self.q_end = q_end
        self.s_start = s_start
        self.s_end = s_end
        self.evalue = evalue
        self.bit_score = bit_score
        self.query_length = query_length
        self.subject_length = subject_length
        self.subject_strand = subject_strand
        self.query_frame = query_frame
        self.subject_frame = subject_frame
        self.qseq_path = qseq_path
        self.sseq_path = sseq_path

    def __repr__(self):
        return json.dumps(self.__dict__)
