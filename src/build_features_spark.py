from spark_initializer import *
from import_data_spark import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, DoubleType, IntegerType
from itertools import combinations
from difflib import SequenceMatcher
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer, OneHotEncoder


"""
PRE-PROCESSING AND DATA CLEANING -------------------------------------------
"""


def replace_values(data, column, mapping_dict):
    """Replaces a certain value by another for an entire column
    using a dictionary for mapping"""
    replace_udf = udf(lambda x: mapping_dict[x] if x in mapping_dict.keys() else x, StringType())
    return data.withColumn(column , replace_udf(data[column]))


def check_for_similar_values(data, column):
    """Only for categorical variables
    """
    unique_values = [x[column] for x in data.select(column).distinct().collect()]
    similar = []
    pairs = [comb for comb in combinations(unique_values, 2)]

    for pair in pairs:
        sim = SequenceMatcher(None, pair[0], pair[1]).ratio()
        if sim >= 0.85 and sim != 1.0:
            similar.append([pair[0], pair[1], sim])

    return similar


def find_unacceptable_values(data, column, criteria):
    """The criteria can be a list of values or a condition in a string
    in the format '> 5'
    """
    if isinstance(criteria, list):
        unique_values = [x[column] for x in data.select(column).distinct().collect()]
        invalid_values = list(set(unique_values) - set(criteria))
        invalid_rows = data.filter(data[column].isin(invalid_values))

    else:
        command = 'invalid_rows = data.filter(data[column]' + criteria + ')'
        exec command

    return invalid_rows


def scale_data(data, features_column):
    """ Scale numerical features with standard scaler
    """
    scaler = StandardScaler(inputCol=features_column, outputCol="features_scaled")
    model = scaler.fit(data)
    scaled_data = model.transform(data)

    return scaled_data, scaler


def change_column_type(data, columns, new_type):
    if isinstance(columns, list):
        for column in columns:
            data = data.withColumn(column, data[column].cast(new_type))
    else:
        data = data.withColumn(columns, data[columns].cast(new_type))
    return data


def build_features(data, columns_to_ignore):
    assembler = VectorAssembler(
        inputCols=[x for x in data.columns if x not in columns_to_ignore],
        outputCol='features')
    new_data = assembler.transform(data)

    return new_data


def encode_categorical_features(data, categorical_columns=None, load_from_file=False):

    if categorical_columns is None:
        categorical_columns = data.columns

    elif not isinstance(categorical_columns, list):
        categorical_columns = [categorical_columns]

    if load_from_file == False:
        if isinstance(categorical_columns, list):
            for column in categorical_columns:
                string_indexer = StringIndexer(inputCol=column, outputCol=column + "_indexed")
                model = string_indexer.fit(data)
                indexed_data = model.transform(data)
                encoder = OneHotEncoder(inputCol=column + "_indexed",
                                        outputCol=column + "_encoded")
                data = encoder.transform(indexed_data)

                onehotEncoder_path = '../models/' + column + '_encoder.csv'
                encoder.save(onehotEncoder_path)
                indexer_path = '../models/' + column + '_indexer.csv'
                model.save(indexer_path)
    else:
        if isinstance(categorical_columns, list):
            for column in categorical_columns:
                onehotEncoder_path = '../models/' + column + '_encoder.csv'
                indexer_path = '../models/' + column + '_indexer.csv'

                loaded_indexer = StringIndexer.load(indexer_path)
                loaded_encoder = OneHotEncoder.load(onehotEncoder_path)

                indexed_data = loaded_indexer.transform(data)
                data = loaded_encoder.transform(indexed_data)

    return data


def save_processed_data(data, filename):
    data.write.csv('../data/processed/'+filename+'.csv')


if __name__ == '__main__':
    raw_data = import_csv_data('../data/raw/houses.csv', header='file')
    # print raw_data.describe()

    numerical_columns = ['price', 'sqm', 'years', 'bedrooms', 'bathrooms']
    categorical_columns = ['property_type', 'new_build', 'estate_type', 'town', 'district',
                           'transaction_category', 'en_suite']

    # new_data = replace_values(raw_data, 'property_type', {'flat': 'FLAT', 'terraced': 'TERRACED'})

    changed_type = change_column_type(raw_data, ['price', 'years', 'sqm', 'bedrooms', 'bathrooms'], DoubleType())

    # save_processed_data(changed_type, 'processed_houses')

    # new_data = build_features(changed_type, ['_c0', 'date', 'postcode', 'property_type',
    #                                          'new_build', 'estate_type', 'number', 'street', 'town',
    #                                          'district', 'transaction_category', 'Adress', 'index',
    #                                          'en_suite', 'price'])

    # scaled_data, scaler = scale_data(new_data, 'features')
    # scaled_data.show()

    new_data = encode_categorical_features(raw_data, 'property_type', load_from_file=True)
    new_data.show()

    # print check_for_similar_values(new_data, 'property_type')
    # invalid = find_unacceptable_values(new_data, 'property_type', '!="FLAT"')
    # invalid.show()

    # new_data = build_features(raw_data, ['date', 'postcode', 'property_type', 'new_build',
    #                                      'estate_type', 'number', 'street', 'town', 'district',
    #                                      'transaction_category', 'Adress', 'index'])
    # new_data.show()

    # print raw_data.head()
    # converted_data = replace_values(raw_data, 'town', 'LONDON', 'CARDIF')
    # unique_towns = check_for_similar_values(converted_data, 'town')
    # print unique_towns
    # print converted_data.head()
    # save_processed_data(raw_data, 'processed_houses')
    # print converted_data['town'].unique()
    # scaled, scaler = scale_data(raw_data, numerical_columns)
    # print '------SCALED DATA----------------------------------------------------'
    # print scaled.head()

    # test = encode_categorical_features(raw_data, categorical_columns=categorical_columns)
    # print '------SCALED DATA----------------------------------------------------'
    # print test