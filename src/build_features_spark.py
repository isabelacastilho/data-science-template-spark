from spark_initializer import *
from import_data_spark import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, DoubleType, IntegerType
from itertools import combinations
from difflib import SequenceMatcher
from pyspark.mllib.feature import StandardScaler, StandardScalerModel
from pyspark.ml.feature import VectorAssembler



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


def scale_data(data, features_column, label_column):
    """ Scale numerical features with standard scaler
    """
    label = data.map(lambda x: x[label_column])
    features = data.map(lambda x: x[features_column])
    scaler = StandardScaler(inputCol=features_column, outputCol=features_column+'_scaled')\
        .fit(features)
    scaled_data = label.zip(scaler.transform(features))

    # numerical_data = data[numerical_columns]
    # transformed_column_names = [x + '_scaled' for x in numerical_columns]
    # scaler = preprocessing.StandardScaler().fit(numerical_data)
    # scaled_data = pd.DataFrame(scaler.transform(numerical_data), columns=transformed_column_names)
    # data = pd.merge(data, scaled_data, left_index=True, right_index=True)

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


def save_processed_data(data, filename):
    data.write.csv('../data/processed/'+filename+'.csv')


if __name__ == '__main__':
    raw_data = import_csv_data('../data/raw/houses.csv', header='file')
    print raw_data.describe()

    numerical_columns = ['price', 'sqm', 'years', 'bedrooms', 'bathrooms']
    categorical_columns = ['property_type', 'new_build', 'estate_type', 'town', 'district',
                           'transaction_category', 'en_suite']

    # new_data = replace_values(raw_data, 'property_type', {'flat': 'FLAT', 'terraced': 'TERRACED'})

    changed_type = change_column_type(raw_data, ['price', 'years'], IntegerType())

    save_processed_data(changed_type, 'processed_houses')

    new_data = build_features(changed_type, ['_c0', 'date', 'sqm', 'postcode', 'property_type',
                                             'new_build', 'estate_type', 'number', 'street', 'town',
                                             'district', 'transaction_category', 'Adress', 'index',
                                             'bedrooms', 'bathrooms', 'en_suite'])
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