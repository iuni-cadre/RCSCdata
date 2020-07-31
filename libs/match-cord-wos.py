#!/usr/bin/env python
# coding: utf-8
import sys, os
from fuzzywuzzy import fuzz
import pandas as pd
from tqdm import tqdm
import requests
import json
import aiohttp
import asyncio
import nest_asyncio
import numpy as np

nest_asyncio.apply()
import logging

# Set up the logger
logger = logging.getLogger("WOS-MATCH")
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(20)

#
# Sub-routines
#
async def fetch(paper, es_end_point):

    #
    # Sub routines
    #
    def make_query(paper):
        """
        Make a query for the ElasticSearch
        
        Parameters
        ----------
        paper: dict 
            with keys, title, authors, year
        
        Returns
        -------
        query: str
            query for the ElasticSearch
        paper_queried: dict
            paper metadata to make the query
        """

        paper_queried = {"title": "", "author_list": [], "year": np.nan}

        original_title = str(paper["title"]).lower()
        original_author_list = paper.get("authors", [])
        original_authors = " ".join(
            original_author_list[: np.minimum(3, len(original_author_list))]
        )

        # Initialize clause place holders
        should_clause = []
        must_clause = []

        # Title
        title_query = '{"match": {"doc.titles.title._VALUE":"%s"}}' % original_title
        must_clause += [title_query]

        # Authors
        for name in original_author_list:
            should_clause += ['{"match": {"doc.name.display_name":"%s"}}' % name]

        # Year of publication
        year = np.nan
        if np.isnan(paper["year"]) == False:
            year = int(paper["year"])
            must_clause += ['{"match": {"doc.pub_info._pubyear":"%d"}}' % year]

        if (len(must_clause) + len(should_clause)) == 1:
            # query = '{"size":%d,"query": %s }' %  (num, title_query)
            query = '{"size":2,"query": { "bool":{ "must":[%s] } }}' % (
                ",".join(must_clause)
            )
        elif len(should_clause) == 0:
            # query = '{"size":%d,"query": %s }' %  (num, title_query)
            query = '{"size":2,"query": { "bool":{ "must":[%s] } }}' % (
                ",".join(must_clause)
            )
        else:
            query = (
                '{"size":2,"query": { "bool":{ "must":[%s], "should":[%s], "minimum_should_match": 1 } }}'
                % (",".join(must_clause), ",".join(should_clause))
            )

        paper_queried["title"] = original_title
        paper_queried["author_list"] = original_author_list
        paper_queried["year"] = year

        return query, paper_queried

    def parse_response(_response):
        """
        Parse the response from the ElasticSearch
        
        Parameters
        ----------
        _response: response
            response from the ElasticSearch
        
        Returns
        -------
        results: list
            List of matches. Each match is a dict object containing authors, title, journal, identifier, year and score.
        """

        _response = json.loads(_response)
        try:
            hits = _response.get("hits", None)
        except:
            print(_response)

        if hits is None:
            return []

        hits = hits["hits"]
        results = []
        for hit in hits:
            score = hit["_score"]
            doc = hit["_source"]["doc"]

            journal = ""
            title = ""
            if doc.get("titles", None) is not None:
                for d in doc["titles"][0]["title"]:
                    if d["_type"] == "source":
                        journal = d["_VALUE"]
                    if d["_type"] == "item":
                        title = d["_VALUE"]

            identifier = doc.get("identifier", [[]])[0]
            UID = doc.get("UID", "")

            authors = []
            if doc.get("name", None) is not None:
                for d in doc["name"][0]:
                    authors += [d["display_name"]]

            year = doc.get("pub_info", [{"_pubyear": np.nan}])[0]["_pubyear"]

            results += [
                {
                    "authors": authors,
                    "title": str(title),
                    "journal": journal,
                    "identifier": identifier,
                    "year": year,
                    "score": score,
                    "UID": UID,
                }
            ]

        return results

    def calculate_similarity(results, paper_queried):
        """
        Calculate the similarity between the hit and query
        
        Parameters
        ----------
        results: list
            result given by parse_response 
        paper_queried:
            dict object that contains the metadata to make the query
            
        Returns
        -------
        matches: list
            List of dict. Each dict consists of "match" and "rank", where match contains the information on the paper found by ElasticSarch
            The "rank" indicates the rank of the match starting from 0 (best match)
        """
        if len(results) == 0:
            return []

        # Compute the similarity for the secondary check
        for rid, res in enumerate(results):
            year_similarity = -10 * np.abs(
                paper_queried["year"]
                - (
                    paper_queried["year"]
                    if np.isnan(res.get("year"))
                    else res.get("year")
                )
            )
            title_similarity = fuzz.ratio(
                paper_queried["title"], res.get("title", " ").lower()
            )
            author_similarity = fuzz.token_sort_ratio(
                " ".join(paper_queried["author_list"]),
                " ".join(res.get("authors", [])).lower(),
            )
            results[rid]["score"] = {
                "title": title_similarity,
                "author": author_similarity,
                "year": year_similarity,
            }

        results = sorted(
            results,
            key=lambda x: -(
                x["score"]["title"]
                + 10 * x["score"]["author"]
                + 10 * x["score"]["year"]
            ),
        )

        best_hit = results[0]
        if len(results) > 1:
            second_hit = results[1]
        else:
            second_hit = {"score": {"title": 0, "author": 0, "year": 0}}

        matches = [{"match": best_hit, "rank": 0}, {"match": second_hit, "rank": 1}]
        return matches

    #
    # Main routine
    #
    query, paper_queried = make_query(paper)
    try:
        headers = {
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                es_end_point, headers=headers, data=query.encode("utf-8")
            ) as response:
                results = parse_response(await response.text())
                results = calculate_similarity(results, paper_queried)
                return results

    except Exception as e:
        logger.error("Unable to get due to {}.".format(e.__class__))
        return -1


async def paper2doi(paper_list, es_end_point):
    ret = await asyncio.gather(*[fetch(paper, es_end_point) for paper in paper_list])
    return ret


if __name__ == "__main__":
    """
    python3 match-cord-wos.py f1 f2 es_end_point username password o1

    ----
    f1 : tab separated file
        Each row corresponds to a paper composed of the following columns:
        - title : name of title
        - year : year of publication
        - volume : volume of paper
        - paper_id : ID of the paper (integer preferred)
    f2 : tab separated file
        Each row corresponds to author-paper pair composed of the following columns:
        - paper_id : ID of a paper (the same id as in f1)
        - author : name of AN author
        Example: 1 paper with three authors A, B, C:
        1   A
        1   B
        1   C
    es_end_point : string
        iuni2.carbonate.uits.iu.edu:9200/wos/_search/ would works 
    username : string
    password : string
    o1 : output file
        Composed of (at least) the following columsn:
        - xref_doi : doi of the paper in the WOS database
        - pmid : pubmed id
        - paper_id : Paper ID for the WOS
        - score_title_0 : Levenshtein distance for the query title
        - score_author_0 : Levenshtein distance for the query author 
        - score_title_1, score_author_1 : Levenshtein distance for the 
            query title and authors of the secondary hit.  (this code picks 
            the two best hits for a query. If these scores are close to score_title_0 and score_title_author_0, cautions should be made because there may be multiple papers that are close to the quary paper).
        - year_1, year_0 : difference in the year of publications for the query paper
    """

    PAPER_FILE = sys.argv[1]  # data_dir = "../data/covid-19/cord"
    PAPER_AUTHOR_AFFIL_FILE = sys.argv[2]
    ES_END_POINT = sys.argv[3]  # 'iuni2.carbonate.uits.iu.edu:9200/wos/_search/'
    ES_USERNAME = sys.argv[4]
    ES_PASSWORD = sys.argv[5]
    OUTPUT = sys.argv[6]

    # Set the end point for the ElasticSearch
    es_end_point = "http://{user}:{password}@{endpoint}".format(
        user=ES_USERNAME, password=ES_PASSWORD, endpoint=ES_END_POINT
    )

    # Load the paper author affiliation file
    paper_author = pd.read_csv(PAPER_AUTHOR_AFFIL_FILE, sep="\t")
    paper2author = paper_author.set_index("paper_id")

    # Initialize the counter
    first_write = True
    paper_count = 0
    identified_count = 0

    # Read the chunks of data 
    for papers in pd.read_csv(PAPER_FILE, sep="\t", chunksize=1000):

        # Convert pandas data frame into list
        paper_list = papers.to_dict("records")
        for i in range(len(paper_list)):
            pid = papers.index[i]
            try:
                authors = paper2author.loc[pid, "author"].values
            except:
                authors = []
            paper_list[i]["authors"] = authors
            paper_list[i]["pid"] = pid

            for new_column in ["doi", "pmid", "xref_doi", "art_no"]:
                paper_list[i][new_column] = ""

        # Search
        loop = asyncio.get_event_loop()
        result = asyncio.run(paper2doi(paper_list, es_end_point))

        # Set identifier if found
        for i in range(len(result)):
            for match in result[i]:  # result contans the best match and secondary math
                if match["rank"] == 0:  # if the match is the best match
                    for identifier in match["match"]["identifier"]:
                        if identifier["_type"] in ["doi", "pmid", "xref_doi", "art_no"]:
                            paper_list[i][identifier["_type"]] = identifier["_value"]
                    paper_list[i]["UID"] = match["match"]["UID"]
                    identified_count += 1

                for sim_type in ["title", "author", "year"]:
                    paper_list[i]["score_%s_%d" % (sim_type, match["rank"])] = match[
                        "match"
                    ]["score"][sim_type]

        papers = pd.DataFrame(paper_list).drop(columns=["authors", "DOI"])

        # Save this chunk
        if first_write:
            first_write = False
            papers.to_csv(OUTPUT, index=False, mode="w")
        else:
            papers.to_csv(OUTPUT, index=False, header=False, mode="a")

        # Logging
        paper_count += papers.shape[0]
        info = "{identified}/{total} identified".format(
            identified=identified_count, total=paper_count
        )
        logger.info(info)
