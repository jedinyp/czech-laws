import os
import requests
import json
import pandas as pd
from .config import Config

def fetch_collection(id: int, output_dir: str = Config.output_dir, to_csv: bool = False, limit: int = 10000) -> pd.DataFrame:
    """
    Function for fetching list of legal documents as DataFrame.

    Args:
        id (int): Id of the collection. Fetch valid ids with catalogues.fetch_catalogue().
        output_dir (str, optional): Path to the output directory. Defaults to Config.output_dir.
        to_csv (bool, optional): Whether to save the reference tables to csv. Defaults to False.
        limit (int, optional): Maximum number of documents to fetch. Defaults to 10000 to fetch all of them.
    
    Returns:
        pd.DataFrame: DataFrame with fetched documents.
    """

    url = "https://www.e-sbirka.cz/sbr-cache/vecne-rejstriky/asociovane-dokumenty-sbirky"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "klicKonceptuCzechVoc": id,
        "pocet": limit
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    data = response.json()

    # Convert to DataFrame
    df = pd.DataFrame(data.get("seznam", []))
    df["freshUrl"] = df["staleUrl"].apply(lambda x: f"https://www.e-sbirka.cz{x}")
    
    # Optionally save to CSV
    if to_csv:
        df.to_csv(os.path.join(output_dir, "collection.csv"), index=False)

    return df