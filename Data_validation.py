import pandas as pd
import logging

# Configure logging
logging.basicConfig(filename='data_validation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def load_data(file_path):
    """Load dataset from a file path."""
    try:
        data = pd.read_csv(file_path)
        logging.info("Data loaded successfully for validation.")
        return data
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None


def check_null_values(df):
    """Check for null values in the DataFrame."""
    try:
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls > 0:
            logging.warning(f"Found {total_nulls} missing values across columns.")
        else:
            logging.info("No missing values found.")
        return null_counts
    except Exception as e:
        logging.error(f"Error checking null values: {e}")
        return None


def check_unique_values(df, columns=None):
    """Check for unique values in specified columns."""
    try:
        unique_counts = {}
        if columns is None:
            columns = df.columns
        for col in columns:
            unique_counts[col] = df[col].nunique()
            logging.info(f"Column '{col}' has {unique_counts[col]} unique values.")
        return unique_counts
    except Exception as e:
        logging.error(f"Error checking unique values: {e}")
        return None


def check_value_ranges(df, column, min_val=None, max_val=None):
    """Check if values in a column fall within a specified range."""
    try:
        out_of_range = False
        if min_val is not None and (df[column] < min_val).any():
            logging.warning(f"Values in '{column}' are below the minimum value {min_val}.")
            out_of_range = True
        if max_val is not None and (df[column] > max_val).any():
            logging.warning(f"Values in '{column}' are above the maximum value {max_val}.")
            out_of_range = True
        if not out_of_range:
            logging.info(f"All values in '{column}' are within the specified range.")
    except Exception as e:
        logging.error(f"Error checking value ranges for column '{column}': {e}")


def check_duplicates(df):
    """Check for duplicate records in the DataFrame."""
    try:
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            logging.warning(f"Found {duplicate_count} duplicate records.")
        else:
            logging.info("No duplicate records found.")
        return duplicate_count
    except Exception as e:
        logging.error(f"Error checking for duplicates: {e}")
        return None


def data_validation_pipeline(file_path):
    """Main function to run the data validation pipeline."""
    df = load_data(file_path)
    if df is not None:
        check_null_values(df)
        check_unique_values(df)
        check_value_ranges(df, column='Age', min_val=0, max_val=100)  # Example column
        check_duplicates(df)
        logging.info("Data validation pipeline executed successfully.")
    else:
        logging.error("Data validation pipeline failed due to data loading issues.")


if __name__ == "__main__":
    # Generate sample data for testing
    sample_data = {
        "Name": ["Alice", "Bob", "Charlie", "Alice"],
        "Age": [25, 35, None, 28],
        "Salary": [50000, 60000, 70000, 50000],
        "Department": ["HR", "Finance", "IT", "HR"]
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df.to_csv("sample_validation_data.csv", index=False)

    # Run the pipeline
    data_validation_pipeline("sample_validation_data.csv")
