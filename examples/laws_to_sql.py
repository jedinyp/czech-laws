from czech_laws.references import fetch_refs
from czech_laws.collections import fetch_collection
from czech_laws.details import fetch_details
from czech_laws.context import fetch_context
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import json

# Database config
DATABASE_URL = "sqlite:///czech_laws.db"
engine = create_engine(DATABASE_URL)

# Helper functions
def serialize_non_primitives(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts list/dict columns into JSON strings so they can be safely stored in SQL.
    """
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
    return df

def add_missing_columns(df: pd.DataFrame, table_name: str):
    """
    Add missing columns to an existing table to allow appending new data.
    Refreshes columns from the DB each time to prevent duplicates.
    """
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return

    # Refresh columns from database
    existing_cols = [col['name'] for col in inspector.get_columns(table_name)]
    missing_cols = [col for col in df.columns if col not in existing_cols]
    if not missing_cols:
        return

    with engine.begin() as conn:
        for col in missing_cols:

            # Refresh columns inside transaction to avoid duplicates
            current_cols = [c['name'] for c in inspect(engine).get_columns(table_name)]
            if col not in current_cols:
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT'))


def snake_to_camel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all column names of a DataFrame from snake_case to camelCase.
    """
    def convert(name: str) -> str:
        parts = name.split('_')

        # Keep first part lowercase, capitalize the rest
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])
    
    df = df.rename(columns={col: convert(col) for col in df.columns})
    return df

# Main script
if __name__ == "__main__":
    
    # Fetch reference tables
    reference_tables = fetch_refs(to_csv=False)
    print("Fetched reference tables:", list(reference_tables.keys()))

    # Push reference tables to SQL
    for name, df in reference_tables.items():
        print(f"Pushing reference table '{name}' to SQL...")
        df = serialize_non_primitives(df)
        df["timestamp"] = pd.Timestamp.now().isoformat()
        df = snake_to_camel(df)
        df.to_sql(name, engine, if_exists="replace", index=False)

    # Get catalogue and iterate codes
    catalogue_df = reference_tables["catalogue"]
    for code in catalogue_df["code"]:
        print("Using code ", code)

        # Fetch collection and push to SQL
        collection_df = fetch_collection(code)
        collection_df = serialize_non_primitives(collection_df)
        collection_df["catalogueCode"] = code
        collection_df["timestamp"] = pd.Timestamp.now().isoformat()
        collection_df = snake_to_camel(collection_df)
        collection_df.to_sql("collection", engine, if_exists="replace", index=False)
        print(f"Fetched and stored {len(collection_df)} documents for code {code}")

        # Fetch details for each document
        for idx, doc in collection_df.iterrows():
            print(f"Fetching details for document {doc['staleUrl']}...")
            details = fetch_details(doc["staleUrl"], to_csv=False)

            # Push content table to SQL
            content_df = details.get("content")
            if isinstance(content_df, pd.DataFrame) and not content_df.empty:
                content_df = serialize_non_primitives(content_df)
                add_missing_columns(content_df, "details")
                content_df["parentStaleUrl"] = doc["staleUrl"]
                content_df["timestamp"] = pd.Timestamp.now().isoformat()
                content_df = snake_to_camel(content_df)
                content_df.to_sql("details", engine, if_exists="append", index=False)

            # Push citations table to SQL
            citations_df = details.get("citations")
            if isinstance(citations_df, pd.DataFrame) and not citations_df.empty:
                citations_df = serialize_non_primitives(citations_df)
                add_missing_columns(citations_df, "citations")
                citations_df["parentStaleUrl"] = doc["staleUrl"]
                citations_df["timestamp"] = pd.Timestamp.now().isoformat()
                citations_df = snake_to_camel(citations_df)
                citations_df.to_sql("citations", engine, if_exists="append", index=False)

            # Fetch context and push to SQL
            context_df = fetch_context(doc["staleUrl"])
            if isinstance(context_df, pd.DataFrame) and not context_df.empty:
                context_df = serialize_non_primitives(context_df)
                add_missing_columns(context_df, "context")
                context_df["parentStaleUrl"] = doc["staleUrl"]
                context_df["timestamp"] = pd.Timestamp.now().isoformat()
                context_df = snake_to_camel(context_df)
                context_df.to_sql("context", engine, if_exists="append", index=False)               

        print("All data successfully pushed to SQL database.")

# TODO - create dspace scraper https://dspace.cuni.cz/handle/20.500.11956/1938 (use with VPN to avoid expulsion)
