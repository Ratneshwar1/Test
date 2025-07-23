import pandas as pd
import logging

# Step 1: Logging setup
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_data(filepath):
    logging.info("Starting data extraction...")
    try:
        df = pd.read_csv(filepath)
        logging.info("Data extraction completed.")
        return df
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise

def transform_data(df):
    logging.info("Starting data transformation...")
    try:
        # Remove duplicates
        df.drop_duplicates(inplace=True)

        # Fill missing values with "Unknown"
        df.fillna("Unknown", inplace=True)

        logging.info("Data transformation completed.")
        return df
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise

def load_data(df, output_path):
    logging.info("Starting data load...")
    try:
        df.to_excel(output_path, index=False)
        logging.info("Data successfully written to Excel.")
    except Exception as e:
        logging.error(f"Load failed: {e}")
        raise

if __name__ == "__main__":
    input_path = "input.csv"
    output_path = "output.xlsx"

    logging.info("Pipeline started.")
    data = extract_data(input_path)
    cleaned_data = transform_data(data)
    load_data(cleaned_data, output_path)
    logging.info("Pipeline completed.")
