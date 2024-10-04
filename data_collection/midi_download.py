import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def download_midi_files(df, midi_directory):  
    """
    This function takes a DataFrame containing URLs of MIDI files and downloads the corresponding files to a specified directory.
    The files are named based on the DataFrame's index, ensuring uniqueness.
    """
    def download_file(url, index):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP request errors
            filepath = os.path.join(midi_directory, f'{index}.mid')
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, mode="wb") as f:
                f.write(response.content)
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
    
    urls = df['Midi'].to_list()  
    with ThreadPoolExecutor() as executor:
        executor.map(download_file, urls, df.index.to_list())


if __name__ == "__main__":
    # import dataset
    wd = os.getcwd()
    lute_df = pd.read_csv(os.path.join(wd, 'data', 'data_sets', 'lute_data.csv'))

    # select subset of the dataset containing the pieces you want to download: for example, the first 10 pieces by Hans Newsidler
    lute_df_filtered = lute_df.query(
        'Composer == "Hans Newsidler"'
    ).head(10)

    # set the directory to which you want to download the files and call the download function
    midi_directory = os.path.join(wd, 'data', 'midi_files')
    download_midi_files(lute_df_filtered, midi_directory)
