import re
import os
import requests
import pandas as pd
from .config import Config

def fetch_details(stale_url: str, output_dir: str = Config.output_dir, to_csv: bool = False) -> dict:
    """
    Function for fetching details of a legal document.

    Args:
        stale_url (str): Stale url of the document.
        output_dir (str, optional): Path to the output directory. Defaults to Config.output_dir.
        to_csv (bool, optional): Whether to save the reference tables to csv. Defaults to False.

    Returns:
        dict: Dictionary containing full html content and citations.
    """

    stale_url = stale_url.replace("/", "%2F")
    url = f"https://www.e-sbirka.cz/sbr-cache/dokumenty-sbirky/{stale_url}"

    # Prepare main df
    response = requests.get(url)
    general_df = pd.DataFrame([response.json()])

    # Fetch fragments
    response = requests.get(url + "/fragmenty?cisloStranky=0")
    data = response.json()
    pages = int(data.get("pocetStranek"))
    pages_data = [data.get("seznam")]

    # Fetch other pages
    if pages is not None and pages > 1:
        for page_number in range(1, pages - 1):
            response = requests.get(url + f"/fragmenty?cisloStranky={page_number}")
            pages_data.append(response.json().get("seznam"))

    # Prepare citations DataFrame
    pages_data = [{"partialCitation": row.get("zkracenaCitace"),
                   "fullCitation": row.get("uplnaCitace"),
                   "htmlContent": row.get("xhtml")} for page in pages_data for row in page]

    # Add html content to main DataFrame
    pages_df = pd.DataFrame(pages_data).fillna("")
    htmlContent =  "<br>".join(pages_df["htmlContent"])
    htmlContent = re.sub(r'[“”„”❝❞]', '', htmlContent)
    general_df["fullHtml"] = htmlContent

    # Optionally save to CSV
    if to_csv:
        general_df.to_csv(os.path.join(output_dir, "general.csv"), index=False)
        pages_df.to_csv(os.path.join(output_dir, "pages.csv"), index=False)

    return {
        "content": general_df,
        "citations": pages_df
    }