from czech_laws.references import fetch_refs
from czech_laws.collections import fetch_collection
from czech_laws.details import fetch_details

# Example usage 
if __name__ == "__main__":
    reference_tables = fetch_refs(to_csv=False)
    print(reference_tables)

    catalogue = reference_tables["catalogue"]

    # Extract code
    code = catalogue.loc[catalogue["name"] == "Daně z příjmů", "code"].item()
    
    # Fetch legal documents with code
    collection = fetch_collection(code)
    doc = collection.iloc[0]
    print(doc)

    # Fetch doc details and save to csv
    details = fetch_details(doc["staleUrl"], to_csv=True)
    print(details)