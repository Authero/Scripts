import yfinance as yf
import pandas as pd
import os
from datetime import datetime

def fetch_technical_data(ticker, start_date, end_date):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        hist.reset_index(inplace=True)  # Make 'Date' a column
        hist['ticker'] = ticker  # Add ticker column to identify the stock
        hist['Date'] = pd.to_datetime(hist['Date']).dt.date  # Ensure Date is in date format
        return hist
    except Exception as e:
        print(f"Error fetching technical data for {ticker}: {e}")
        return None

def fetch_fundamental_data(ticker, start_date):
    try:
        stock = yf.Ticker(ticker)
        # Get quarterly income statements
        quarterly_income = stock.quarterly_income_stmt

        # Transpose the DataFrame for easier manipulation
        quarterly_income = quarterly_income.T

        # Convert index to datetime and filter based on start_date
        quarterly_income.index = pd.to_datetime(quarterly_income.index)
        start_date_dt = pd.to_datetime(start_date)

        # Filter quarters that are on or after the start_date
        filtered_income = quarterly_income[quarterly_income.index >= start_date_dt]

        # Reset index to include the date as a column
        filtered_income.reset_index(inplace=True)
        filtered_income.rename(columns={'index': 'Date'}, inplace=True)
        filtered_income['Date'] = filtered_income['Date'].dt.date  # Ensure Date is in date format

        # Add ticker column
        filtered_income['Ticker'] = ticker

        return filtered_income
    except Exception as e:
        print(f"Error fetching fundamental data for {ticker}: {e}")
        return None

def fetch_sentiment_data(ticker):
    # Placeholder for sentiment data (e.g., news headlines)
    # Replace with actual implementation (e.g., NewsAPI)
    sentiment_data = {
        'ticker': ticker,
        'date': datetime.now().strftime('%Y-%m-%d'),
        'headline': f"Sample headline for {ticker}",
        'text': f"Sample text for {ticker}"
    }
    return pd.DataFrame([sentiment_data])

def load_existing_data(file_path):
    if os.path.exists(file_path):
        existing_data = pd.read_csv(file_path)
        if 'Date' in existing_data.columns:
            existing_data['Date'] = pd.to_datetime(existing_data['Date']).dt.date
        return existing_data
    return pd.DataFrame()

def append_new_data(existing_data, new_data):
    if existing_data.empty or new_data.empty:
        return new_data
    # Ensure 'Date' and 'ticker' columns exist for comparison
    if 'Date' in existing_data.columns and 'Date' in new_data.columns and 'ticker' in existing_data.columns and 'ticker' in new_data.columns:
        # Convert 'Date' to datetime.date for comparison
        existing_data['Date'] = pd.to_datetime(existing_data['Date']).dt.date
        new_data['Date'] = pd.to_datetime(new_data['Date']).dt.date
        # Merge to find rows in new_data that are not in existing_data
        merged = pd.merge(new_data, existing_data, on=['Date', 'ticker'], how='left', indicator=True)
        new_rows = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
        return pd.concat([existing_data, new_rows], ignore_index=True)
    else:
        return new_data

def main(start_date, end_date=None):
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    # Read input tickers from CSV
    input_path = os.path.join(os.getcwd(), 'source', 'data', 'tickers.csv')
    tickers_df = pd.read_csv(input_path)

    # Check if 'Ticker' column exists
    if 'Ticker' not in tickers_df.columns:
        raise ValueError("The CSV file must contain a 'Ticker' column.")

    tickers = tickers_df['Ticker'].tolist()

    # Create output directory if it doesn't exist
    output_dir = os.path.join(os.getcwd(), 'output')
    os.makedirs(output_dir, exist_ok=True)

    # Initialize file paths
    technical_file_path = os.path.join(output_dir, 'technical_data.csv')
    fundamental_file_path = os.path.join(output_dir, 'fundamental_data.csv')
    sentiment_file_path = os.path.join(output_dir, 'sentiment_data.csv')

    # Load existing data
    existing_technical_data = load_existing_data(technical_file_path)
    existing_fundamental_data = load_existing_data(fundamental_file_path)
    existing_sentiment_data = load_existing_data(sentiment_file_path)

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")

        # Fetch and append technical data
        technical_data = fetch_technical_data(ticker, start_date, end_date)
        if technical_data is not None:
            updated_technical_data = append_new_data(existing_technical_data, technical_data)
            updated_technical_data.to_csv(technical_file_path, index=False)
            existing_technical_data = updated_technical_data

        # Fetch and append fundamental data (quarterly income statements)
        fundamental_data = fetch_fundamental_data(ticker, start_date)
        if fundamental_data is not None:
            updated_fundamental_data = append_new_data(existing_fundamental_data, fundamental_data)
            updated_fundamental_data.to_csv(fundamental_file_path, index=False)
            existing_fundamental_data = updated_fundamental_data

        # Fetch and append sentiment data
        sentiment_data = fetch_sentiment_data(ticker)
        if sentiment_data is not None:
            updated_sentiment_data = append_new_data(existing_sentiment_data, sentiment_data)
            updated_sentiment_data.to_csv(sentiment_file_path, index=False)
            existing_sentiment_data = updated_sentiment_data

    print("Data saved to output files.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Fetch data for a list of tickers.')
    parser.add_argument('--start_date', type=str, required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', type=str, default=None, help='End date in YYYY-MM-DD format (defaults to today)')

    args = parser.parse_args()
    main(args.start_date, args.end_date)