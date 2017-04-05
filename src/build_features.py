from import_data import import_csv_data
from itertools import combinations
from difflib import SequenceMatcher
import string
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


def replace_values(data, column, mapping_dict):
    """Replaces a certain value by another for an entire column
    using the mapping of a dictionary"""
    for key in mapping_dict.keys():
        data[column] = data[column].replace(key, mapping_dict[key])
    return data


def check_for_similar_values(data, column):
    """Only for categorical variables
    """
    unique_values = data[column].unique()
    similar = []
    pairs = [comb for comb in combinations(unique_values, 2)]

    for pair in pairs:
        sim = SequenceMatcher(None, pair[0], pair[1]).ratio()
        if sim >= 0.85 and sim != 1.0:
            similar.append([pair[0], pair[1], sim])

    return similar


def find_unacceptable_values(data, column, criteria):
    """For categories, the criteria should be a list of values
    For numerical features, it should be a condition in a string
    in the format '> 5'
    """
    if isinstance(criteria, list):
        unique_values = data[column].unique()
        invalid_values = list(set(unique_values) - set(criteria))
        invalid_rows = data.loc[data[column].isin(invalid_values)]

    else:
        command = 'invalid_rows = data.loc[data[column]' + criteria + ']'
        exec command

    return invalid_rows


def simplify_text(data, column, stemming=False, clear_stop_words=False, tokenize=False):
    """
    """
    data[column] = data[column].apply(lambda x: x.translate(None, string.punctuation).lower())

    if tokenize:
        data[column] = data[column].apply(lambda x: word_tokenize(x))

    if stemming:
        ps = PorterStemmer()
        data[column] = data[column].apply(lambda x: ps.stem(word_tokenize(x)))

    if clear_stop_words:
        data[column] = data[column].apply(lambda x: [word for word in ps.stem(word_tokenize(x)) if
                                                     word not in stopwords.words('english')])

    return data


if __name__ == '__main__':
    raw_data = import_csv_data('../data/raw/houses.csv')
    converted_data = replace_values(raw_data, 'town', 'LONDON', 'CARDIF')
    # unique_towns = check_for_similar_values(converted_data, 'town')
    # print unique_towns
    # print converted_data.head()
    print converted_data['town'].unique()