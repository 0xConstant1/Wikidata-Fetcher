import json
import logging
import os
from client import WikidataFetcher

# --- Configuration ---
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Your bot's User-Agent
USER_AGENT = "WikidataFetcher/1.0 (https://github.com/0xConstant1/Wikidata-Fetcher; wikidata.fetcher@mail.com)"

# --- Query Definitions ---
MOVIE_QUERY = """
SELECT ?imdbId ?tmdbId ?tvdbId
WITH {
  SELECT ?item WHERE {
    VALUES ?type {
      wd:Q11424
      wd:Q1261214
      wd:Q202866
      wd:Q506240
      wd:Q24856
      wd:Q844580
    }
    ?item wdt:P31/wdt:P279* ?type.
  }
} AS %typed_items
WHERE {
  INCLUDE %typed_items.
  ?item wdt:P345 ?imdbId.
  OPTIONAL { ?item wdt:P4947 ?tmdbId. }
  OPTIONAL { ?item wdt:P12196 ?tvdbId. }
}
"""

TV_QUERY = """
SELECT ?imdbId ?tvdbId ?tmdbId ?tvmazeId
WITH {
  SELECT ?series WHERE {
    VALUES ?type {
      wd:Q5398426  # television series
      wd:Q581714   # animated series
      wd:Q3464665  # television series season
      wd:Q21191270 # television series episode
      wd:Q15416    # television program
      wd:Q653916   # television pilot
      wd:Q7697093  # television play
    }
    ?series wdt:P31 ?type.
  }
} AS %tv_items
WHERE {
  INCLUDE %tv_items.
  ?series wdt:P345 ?imdbId.
  OPTIONAL { ?series wdt:P4835 ?tvdbId. }
  OPTIONAL { ?series wdt:P4983 ?tmdbId. }
  OPTIONAL { ?series wdt:P8600 ?tvmazeId. }
}
"""

# --- Task Runner Definition ---
# A list of dictionaries, where each dict is a self-contained task.
# This makes it very easy to add more queries in the future.
QUERIES_TO_RUN = [
    {
        "name": "Movie Mappings",
        "query": MOVIE_QUERY,
        "output_file": "data/movie_mappings.json"
    },
    {
        "name": "TV Mappings",
        "query": TV_QUERY,
        "output_file": "data/tv_mappings.json"
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
        
        logging.info(f"--- Starting task: {task_name} ---")
        
        try:
            # MODIFICATION: We now pass use_post=True to the query method.
            results = client.query(task["query"], use_post=True)
            
            if results and 'results' in results and 'bindings' in results['results']:
                num_results = len(results['results']['bindings'])
                logging.info(f"Successfully fetched {num_results} results for {task_name}.")
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                logging.info(f"Data for {task_name} successfully saved to {output_path}")
            else:
                logging.error(f"Query for {task_name} returned no results or malformed data.")
                # We exit with an error code to make sure the GitHub Action fails.
                exit(1)

        except RuntimeError as e:
            logging.error(f"An error occurred during the '{task_name}' task: {e}")
            exit(1)
        
        logging.info(f"--- Finished task: {task_name} ---\n")

if __name__ == "__main__":
    main()