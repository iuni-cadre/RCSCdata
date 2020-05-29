"""
Usage:

    python3 parse-cord-data.py list_of_directory out_paper out_reference

Parameters
----------
list_of_directory: list of directories containing json files. 
   Spase separated.

out_paper: A csv file for the list of papers. 
   The 'paper_id' is assigned to each paper. Note that the same paper may have multiple paper ids as this program
   does not perform entity resolutions.
             
out_reference: A csv file for the list of citations between papers. 

Example
-------

    python3 parse-cord-data.py document_parses/pdf_json document_parses/pmc_json papers.csv references.csv

Requirement
-----------
- tqdm
- pandas

Note
----
If you want to extract other attributes in json files, change focal_field_list in extract_from_json function

"""

import pandas as pd
import json
import glob
import sys
from tqdm import tqdm


def issue_paper_ids():
    """
    Paper ID generator
    """
    paper_id = 0
    while True:
        yield paper_id
        paper_id = paper_id + 1


def extract_from_json(
    filename,
    paper_id_generator,
    focal_field_list=[
        "title",
        "year",
        "venue",
        "volume",
        "issn",
        "pages",
        "cord_paper_id",
        "paper_id",
    ],
):
    """
    Extract the bibliography from the json file
    """
    with open(filename, "r") as f:
        data = json.load(f)

    paper = {f: None for f in focal_field_list}
    paper["cord_paper_id"] = data["paper_id"]
    paper["title"] = data["metadata"]["title"]  # Title
    paper["paper_id"] = next(paper_id_generator)

    raw_references = [
        v
        for k, v in data["bib_entries"].items()
        if ("BIBREF" in k) and v["year"] is not None
    ]  # Extract all references
    references = []
    for i, raw_ref in enumerate(raw_references):
        ref = {fi: raw_ref.get(fi, None) for fi in focal_field_list}
        doi = raw_ref["other_ids"].get("DOI", [None])
        ref["DOI"] = doi[0] if len(doi) > 0 else None
        references += [ref]

    for i, r in enumerate(references):
        references[i]["paper_id"] = next(paper_id_generator)

    return paper, references


if __name__ == "__main__":

    PAPER_REFERENCE_FILE = sys.argv.pop()
    PAPER_FILE = sys.argv.pop()
    DIR_LIST = sys.argv[1:]

    FILE_LIST = sum([glob.glob("%s/*.json" % d) for d in DIR_LIST], [])

    paper_id_generator = issue_paper_ids()
    paper_refs = []
    papers = []
    for filename in tqdm(FILE_LIST):

        # Extract bibliography from json
        paper, references = extract_from_json(filename, paper_id_generator)

        # Record the citations between the paper and its references
        paper_refs += [(paper["paper_id"], ref["paper_id"]) for ref in references]

        # Record extracted papers
        papers += [pd.DataFrame([paper] + references)]

    # Pack into the pandas DataFrame
    papers = pd.concat(papers, ignore_index=True)
    paper_refs = pd.DataFrame(paper_refs, columns=["source", "target"])

    # Save
    papers.to_csv(PAPER_FILE, sep="\t", index=False)
    paper_refs.to_csv(PAPER_REFERENCE_FILE, sep="\t", index=False)
