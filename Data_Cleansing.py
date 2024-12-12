import pandas as pd
import numpy as np
import logging

# Configure logging
logging.basicConfig(filename='data_cleaning.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def load_data(file_path):
    """Load dataset from a file path."""
    try:
        data = pd.read_csv(file_path)
        logging.info("Data loaded successfully.")
        return data
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None


def handle_missing_values(df, strategy="mean"):
    """Handle missing values in the DataFrame."""
    try:
        if strategy == "mean":
            df.fillna(df.mean(), inplace=True)
        elif strategy == "median":
            df.fillna(df.median(), inplace=True)
        elif strategy == "mode":
            df.fillna(df.mode().iloc[0], inplace=True)
        else:
            df.dropna(inplace=True)
        logging.info("Missing values handled successfully.")
    except Exception as e:
        logging.error(f"Error handling missing values: {e}")
    return df


def format_columns(df):
    """Format column names by converting to lowercase and replacing spaces with underscores."""
    try:
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        logging.info("Column formatting applied successfully.")
    except Exception as e:
        logging.error(f"Error formatting columns: {e}")
    return df


def detect_outliers(df, z_threshold=3):
    """Detect and flag outliers based on the Z-score method."""
    try:
        numeric_cols = df.select_dtypes(include=[np.number])
        z_scores = (numeric_cols - numeric_cols.mean()) / numeric_cols.std()
        outliers = (np.abs(z_scores) > z_threshold)
        df['outlier'] = outliers.any(axis=1)
        logging.info("Outlier detection applied successfully.")
    except Exception as e:
        logging.error(f"Error detecting outliers: {e}")
    return df


def clean_data_pipeline(file_path):
    """Main function to run the data cleaning pipeline."""
    df = load_data(file_path)
    if df is not None:
        df = handle_missing_values(df)
        df = format_columns(df)
        df = detect_outliers(df)
        logging.info("Data cleaning pipeline executed successfully.")
        return df
    else:
        logging.error("Data cleaning pipeline failed due to data loading issues.")
        return None


if __name__ == "__main__":
    # Generate sample data for testing
    sample_data = {
        "Name": ["Alice", "Bob", None, "Diana"],
        "Age": [25, np.nan, 30, 22],
        "Salary": [50000, 60000, 70000, None],
        "Department": ["HR", "Finance", "IT", None]
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df.to_csv("sample_data.csv", index=False)

    # Run the pipeline
    cleaned_data = clean_data_pipeline("sample_data.csv")
    if cleaned_data is not None:
        print(cleaned_data)
