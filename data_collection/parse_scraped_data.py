import os
import pandas as pd
import re
import numpy as np
import json

# list of European countries; will be used for parsing strings containing the composer information
countries = [
    'Portugal', 'Spain', 'France', 'Belgium', 'Ireland', 'England', 'United Kingdom', 'Scotland',
    'Netherlands', 'Denmark', 'Germany', 'Switzerland', 'Austria', 'Italy', 'Czechia', 'Czech Republic',
    'Poland', 'Slovakia', 'Hungary', 'Slovenia', 'Croatia', 'Russia', 'Sweden', 'Norway', 'Finland',
    'Romania', 'Belarus', 'Greece', 'Bulgaria', 'Serbia', 'Lithuania', 'Latvia', 'Bosnia and Herzegovina',
    'Slovakia', 'Estonia', 'Albania'
]

def initialize_composer_dict(composers, classical=True):
    """
    Initialize dictionaries for storing composer data.
    """
    composer_dict = {}
    for composer in composers:
        composer_dict[composer] = {
            'composer': composer,
            'date_of_birth': np.nan,
            'birth_town': np.nan,
            'birth_country': np.nan,
            'date_of_death': np.nan,
            'death_town': np.nan,
            'death_country': np.nan
        }
        if not classical:
            composer_dict[composer]['group_country'] = np.nan
    return composer_dict

def extract_dates_and_locations(info_string):
    """
    Extract dates and locations from an info string.
    """
    dates, locations = np.nan, np.nan
    split = re.split('(?<=\d{4}) ', info_string)
    if len(split) > 1:
        dates = split[0]
        locations = split[1]
    return dates, locations

def parse_date(date_str):
    """
    Parse a date string to extract the year.
    """
    year = np.nan
    search = re.search('\d{4}', date_str)
    if search:
        year = search[0]
    return year

def parse_location(location_str):
    """
    Parse a location string to extract the town and country.
    """
    country, town = np.nan, np.nan
    location_parts = re.split(', ', location_str)
    for part in location_parts:
        if part in countries:
            country = part
        else:
            town = part
    return town, country

def extract_info_classical(comp, info_string):
    """
    Extract information for classical composers from an info string.
    """
    info_string = re.sub('^[^\d-]*', 'a', info_string)
    info_string = re.sub('\?|\r', '', info_string)

    if "," in info_string:
        dates, locations = extract_dates_and_locations(info_string)
        if dates and locations:
            for index, cat in enumerate(['birth', 'death']):
                date_str = re.split("-", dates)
                if len(date_str) > 1:
                    composer_dict_classical[comp]['date_of_' + cat] = parse_date(date_str[index])

                location_str = re.split(' - ', locations)
                if len(location_str) > 1:
                    town, country = parse_location(location_str[index])
                    composer_dict_classical[comp][cat + '_country'] = country
                    composer_dict_classical[comp][cat + '_town'] = town

def extract_info_musicalics(cat, comp, div_texts):
    """
    Extract information for musicalics composers from div text.
    """
    div_texts = [x for x in div_texts if re.search('\n|Age|Birth|Death', x) is None]
    for string in div_texts:
        if re.search('\d{4}', string):
            div_texts = div_texts[div_texts.index(string):]
            break

    for string in div_texts:
        if string in countries:
            composer_dict_musicalics[comp][cat + '_country'] = string
        elif re.search('\d{4}', string):
            composer_dict_musicalics[comp]['date_of_' + cat] = re.findall('\d{4}', string)[0]
        elif re.search('[a-z]', string):
            composer_dict_musicalics[comp][cat + '_town'] = string

def extract_group_country(comp, div_texts):
    """
    Extract group country information.
    """
    for string in div_texts:
        if string in countries:
            composer_dict_musicalics[comp]['group_country'] = string


if __name__ == '__main__':
    # load scraped data from JSON
    wd = os.getcwd()
    with open(os.path.join(wd, 'data', 'data_sets', 'composer_data_classical.json'), 'r', encoding='utf-8') as f:
        composer_data_classical = json.load(f)

    with open(os.path.join(wd, 'data', 'data_sets', 'composer_data_musicalics.json'), 'r', encoding='utf-8') as f:
        composer_data_musicalics = json.load(f)

    composers = list(composer_data_classical.keys())

    # initialize dictionaries for parsed data
    composer_dict_classical = initialize_composer_dict(composers, classical=True)
    composer_dict_musicalics = initialize_composer_dict(composers, classical=False)

    # parse classical music data
    for comp in composer_data_classical.keys():
        info = composer_data_classical[comp]
        extract_info_classical(comp=comp, info_string=info)

    # parse musicalics data
    for cat in ['birth', 'death']:
        for comp in composer_data_musicalics[cat].keys():
            div_texts = composer_data_musicalics[cat][comp]
            extract_info_musicalics(cat, comp, div_texts)

    for comp in composer_data_musicalics['group'].keys():
        div_texts = composer_data_musicalics['group'][comp]
        extract_group_country(comp, div_texts)

    # convert dictionaries to DataFrames
    df_comp_data_classical = pd.DataFrame(composer_dict_classical).transpose()
    df_comp_data_classical = df_comp_data_classical.replace(to_replace='', value=np.nan)
    df_comp_data_musicalics = pd.DataFrame(composer_dict_musicalics).transpose()

    # merge the data
    df_comp_data = df_comp_data_musicalics

    # fill missing values in musicalics dataframe with data from the composers-classical-music dataframe
    df_comp_data['date_of_birth'] = df_comp_data['date_of_birth'].fillna(df_comp_data_classical['date_of_birth'])
    df_comp_data['birth_town'] = df_comp_data['birth_town'].fillna(df_comp_data_classical['birth_town'])
    df_comp_data['birth_country'] = df_comp_data['birth_country'].fillna(df_comp_data_classical['birth_country'])

    df_comp_data['date_of_death'] = df_comp_data['date_of_death'].fillna(df_comp_data_classical['date_of_death'])
    df_comp_data['death_town'] = df_comp_data['death_town'].fillna(df_comp_data_classical['death_town'])
    df_comp_data['death_country'] = df_comp_data['death_country'].fillna(df_comp_data_classical['death_country'])

    # create a column "nationality" that favors group_country over birth_country over death_country
    df_comp_data = df_comp_data.rename(columns={'group_country': 'nationality'})
    df_comp_data['nationality'] = df_comp_data['nationality'].fillna(df_comp_data_classical['birth_country'])
    df_comp_data['nationality'] = df_comp_data['nationality'].fillna(df_comp_data_classical['death_country'])

    # export the merged and parsed data to CSV
    output_path = os.path.join(wd, 'data', 'data_sets', 'merged_parsed_composer_data.csv')
    df_comp_data.to_csv(output_path, index=False, encoding='latin-1')
