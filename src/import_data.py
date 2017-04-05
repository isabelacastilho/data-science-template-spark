import pandas as pd


def import_csv_data(file_path):
    data = pd.read_csv(file_path)
    return data


def change_column_names(data, column_names):
    data.columns = column_names
    return data


def select_index_column(data, column_name):
    data.index = data[column_name]
    return data


def get_data_info(data):
    print '\n-------- SCHEMA AND SHAPE --------\n'
    print data.info()
    print '\n---------- NULL VALUES -----------\n'
    print data.isnull().sum()

if __name__ == '__main__':
    raw_data = import_csv_data('../data/raw/houses.csv')
    # raw_data.columns = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
    #                     '11', '12', '13', '14', '15', '16', '17', '18', '19']
    # raw_data = select_index_column(raw_data, '1')
    get_data_info(raw_data)
    print raw_data.head()
