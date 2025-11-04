import os
import time
import requests
import pandas as pd
from .config import Config

def fetch_refs(output_dir: str = Config.output_dir, to_csv: bool = False) -> dict:
    """
    Function for fetching reference tables and saving them to csv.

    Args:
        output_dir (str, optional): Path to the output directory. Defaults to Config.output_dir.
        to_csv (bool, optional): Whether to save the reference tables to csv. Defaults to False.
    
    Returns:
        dict: Dict of reference tables.
    """
    
    # Fetch seed json
    url = "https://www.e-sbirka.cz/sbr-cache/vecne-rejstriky/asociovane-dokumenty-sbirky"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    payload = {
        "klicKonceptuCzechVoc": 196567, # Arbitrary working id
        "pocet": 1
    }
    
    # Fetch data and retry on error
    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
    except:
        time.sleep(10)
        return fetch_refs(output_dir, to_csv)

    # Extract reference tables
    collections = data.get("fazetovyFiltr", {}).get("sbirka", [])
    types = data.get("fazetovyFiltr", {}).get("typPravnihoAktu", [])
    catalogue = data.get("fazetovyFiltr", {}).get("vecnyRejstrik", [])

    def recursive_parse(nodes, level = 0, parent_name = None, parent_code = None):
        """
        Recursive helper function for parsing nested reference tables.
        """
        if nodes is None:
            return []
        result = []
        for node in nodes:
            name = node.get("text")
            code = node.get("kod")
            result.append({
                "name": name,
                "code": code,
                "level": level,
                "parent_name":  parent_name,
                "parent_code": parent_code
            })
            result += recursive_parse(node.get("potomci"), level = level + 1, 
                                      parent_name = name, parent_code = code)
        return result

    # Convert to DataFrame
    categories_df = pd.DataFrame(recursive_parse(collections))
    types_df = pd.DataFrame(recursive_parse(types))
    catalogue_df = pd.DataFrame(recursive_parse(catalogue))

    # Optionally save to CSV
    if to_csv:
        categories_df.to_csv(os.path.join(output_dir, "collections.csv"), index=False)
        types_df.to_csv(os.path.join(output_dir, "types.csv"), index=False)
        catalogue_df.to_csv(os.path.join(output_dir, "catalogue.csv"), index=False)

    return {
        "categories": categories_df,
        "types": types_df,
        "catalogue": catalogue_df
    }
