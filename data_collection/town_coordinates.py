import os
import pandas as pd
import requests
import numpy as np
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_town_coordinates(df, api_key, csv_path):
    """
    Get coordinates for unique towns in the dataframe and save to a CSV file.
    """
    # Get unique towns in the dataframe
    unique_towns = pd.unique(df[['birth_town', 'death_town']].to_numpy().flatten())
    logging.info(f"Found {len(unique_towns)} unique towns.")
    
    # DataFrame for the towns
    towns_df = pd.DataFrame({'town': list(unique_towns)})
    towns_df = towns_df.dropna()

    # Lists to store longs and lats
    longitudes = []
    latitudes = []
    
    # Iterate over the towns DataFrame, call API, and save longitudes and latitudes
    for _, row in towns_df.iterrows():
        town = row['town']
        url = f'http://api.positionstack.com/v1/forward?access_key={api_key}&query={town}&limit=1'
        try:
            response = requests.get(url).json()
            if 'data' in response and len(response['data']) > 0:
                longitudes.append(response['data'][0].get('longitude', np.nan))
                latitudes.append(response['data'][0].get('latitude', np.nan))
            else:
                longitudes.append(np.nan)
                latitudes.append(np.nan)
                logging.warning(f"No data found for town: {town}")
        except Exception as e:
            longitudes.append(np.nan)
            latitudes.append(np.nan)
            logging.error(f"Error retrieving data for town: {town} - {e}")
    
    # Store location data in the towns DataFrame
    towns_df['longitude'] = longitudes
    towns_df['latitude'] = latitudes
    towns_df = towns_df.set_index('town')

    # Export as CSV
    towns_df.to_csv(csv_path, sep=',')
    logging.info(f"Exported town coordinates to {csv_path}")


if __name__ == "__main__":
    # Read composer data
    wd = os.getcwd()
    comp_path = os.path.join(wd, 'data', 'data_sets', 'scraped_composer_data_cleaned.csv')
    csv_path = os.path.join(wd, 'data', 'data_sets', 'town_coordinates.csv')
    comp_df = pd.read_csv(comp_path, index_col='composer', encoding='latin-1')
    
    # API key for positionstack
    API_key = '00fdf2537e11d99fdd97bd5682baa570'
    
    # Get town coordinates and save to CSV
    get_town_coordinates(comp_df, API_key, csv_path)
