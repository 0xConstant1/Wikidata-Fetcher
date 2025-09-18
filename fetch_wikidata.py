import json
import logging
import os
from client import WikidataFetcher

# --- Configuration ---
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# User-Agent
USER_AGENT = "WikidataFetcher/1.0 (https://github.com/0xConstant1/Wikidata-Fetcher; wikidata.fetcher@mail.com)"

# --- Query Definitions ---
MOVIE_QUERY = """
SELECT ?imdbId (SAMPLE(?tvdbId) AS ?tvdbId) (SAMPLE(?tmdbId) AS ?tmdbId) WHERE {
  VALUES ?type {
    wd:Q11424
    wd:Q24856
    wd:Q506240
    wd:Q844580
    wd:Q226730
    wd:Q202866
    wd:Q1261214
  }
  ?item (wdt:P31/(wdt:P279*)) ?type;
    wdt:P345 ?imdbId;
    wdt:P4947 ?tmdbId.
  OPTIONAL { ?item wdt:P12196 ?tvdbId. }
}
GROUP BY ?imdbId
"""

TV_QUERY = """
SELECT ?imdbId (SAMPLE(?tvdbId) AS ?tvdbId) (SAMPLE(?tmdbId) AS ?tmdbId) (SAMPLE(?tvmazeId) AS ?tvmazeId)
WHERE {
  VALUES ?type {
    wd:Q5398426
    wd:Q581714
    wd:Q3464665
    wd:Q21191270
    wd:Q15416
    wd:Q653916
    wd:Q7697093
  }
  ?series wdt:P31/wdt:P279* ?type.
  ?series wdt:P345 ?imdbId.
  { ?series wdt:P4983 ?tmdbId. }
  OPTIONAL { ?series wdt:P4835 ?tvdbId. }
  OPTIONAL { ?series wdt:P8600 ?tvmazeId. }
}
GROUP BY ?imdbId
"""

# --- Task Runner Definition ---
QUERIES_TO_RUN = [
    {
        "name": "Movie Mappings",
        "query": MOVIE_QUERY,
        "output_file": "data/movie_mappings.csv",
        "format": "csv"
    },
    {
        "name": "TV Mappings",
        "query": TV_QUERY,
        "output_file": "data/tv_mappings.csv",
        "format": "csv"
    }
]

def main():
    """
    Runs all predefined Wikidata queries and saves the results.
    """
    logging.info("Initializing Wikidata client...")
    client = WikidataFetcher(user_agent=USER_AGENT)
    
    # Ensure the output directory exists
    output_dir = os.path.dirname(QUERIES_TO_RUN[0]["output_file"])
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for task in QUERIES_TO_RUN:
        task_name = task["name"]
        output_path = task["output_file"]
        task_format = task["format"]
        
        logging.info(f"--- Starting task: {task_name} (format: {task_format}) ---")
        
        try:
            results = client.query(task["query"], use_post=True, format=task_format)
            
            if results:
                logging.info(f"Successfully fetched data for {task_name}.")
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    if task_format == 'csv':
                        # If it's CSV, the result is a string. Just write it.
                        f.write(results)
                    else: # Handle JSON or other formats if added in the future
                        json.dump(results, f, ensure_ascii=False, indent=2)

                logging.info(f"Data for {task_name} successfully saved to {output_path}")
            else:
                logging.error(f"Query for {task_name} returned no results or malformed data.")
                exit(1)

        except RuntimeError as e:
            logging.error(f"An error occurred during the '{task_name}' task: {e}")
            exit(1)
        
        logging.info(f"--- Finished task: {task_name} ---\n")

if __name__ == "__main__":
    main()