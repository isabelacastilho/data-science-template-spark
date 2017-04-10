from spark_initializer import *
from pyspark.sql.functions import col


def import_csv_data(file_path, header='file'):
    if header == 'file':
        data = sqlContext.read.csv(file_path, header=True)
    else:
        data = sqlContext.read.csv(file_path)

    return data


def change_column_names(data, new_names):
    """If a list is passed as new_names, that list needs to contain all the column names
    for the new dataframe. If it's a dictionary, it only needs to contain
    the columns to be changed.
    """
    old_names = data.columns
    if isinstance(new_names, list):
        mapping = dict(zip(old_names, new_names))
    else:
        mapping = new_names

    transformed_data = data.select([col(c).alias(mapping.get(c, c)) for c in data.columns])
    return transformed_data


def get_data_info(data):
    print '\n-------- SCHEMA AND SHAPE --------\n'
    print data.printSchema()
    print '\n----------- DATA STATS -----------\n'
    data.describe().show()
    # print '\n---------- NULL VALUES -----------\n'
    # print data.isnull().sum()


if __name__ == '__main__':
    raw_data = import_csv_data('../data/raw/houses.csv', header='file')
    columns = raw_data.columns
    columns[2] = 'DATE'
    new_data = change_column_names(raw_data, {'price':'PRICE!'})
    new_data.show()