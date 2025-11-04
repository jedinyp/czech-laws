import re
import os
import time
import requests
import pandas as pd
from .config import Config

def fetch_context(stale_url: str, output_dir: str = Config.output_dir, to_csv: bool = False) -> dict:
    """
    Function for fetching context of a legal document.

    Args:
        stale_url (str): Stale url of the document.
        output_dir (str, optional): Path to the output directory. Defaults to Config.output_dir.
        to_csv (bool, optional): Whether to save the reference tables to csv. Defaults to False.

    Returns:
        dict: Dictionary containing full html content and citations.
    """

    stale_url = stale_url.replace("/", "%2F")
    url = f"https://www.e-sbirka.cz/sbr-cache/dokumenty-sbirky/{stale_url}/souvislosti"

    # Fetch data and retry on error
    try:
        response = requests.get(url)
        data = response.json()
    except:
        time.sleep(10)
        return fetch_context(stale_url, output_dir, to_csv)
    
    # Parse and create DataFrame
    context = []
    for row in data.get("souvislosti", []):
        for doc in row.get("dokumentySbirky") + row.get("ostatniDokumentySbirky", []):
            context.append({
                "type": row.get("typ"),
                "name": doc.get("nazev"),
                "staleUrl": doc.get("staleUrl"),
            })
    
    context_df = pd.DataFrame(context)
    
    return context_df
