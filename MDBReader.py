import pyodbc
import pandas as pd


class connect_db():
    """ Connect by default a *.mdb database """

    def __init__(self, fpFile, driver='Microsoft Access Driver (*.mdb, *.accdb)', db_pwd='.'):
        self.fpFile = fpFile
        self.driver = driver
        self.db_pwd = db_pwd
        self.conn_str = f'DRIVER={{{self.driver}}};DBQ={fpFile};PWD={db_pwd}'

    def __enter__(self):
        self.connection = pyodbc.connect(self.conn_str)
        return self.connection

    def __exit__(self, exc_type, exc_val, tracebak):
        self.connection.close()


def query_db_connection(connection, query):
    """ Return a pandas.DataFrame as a result of the query """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = pd.read_sql_query(query, connection)
    return data


def get_collection_property_from_dataframe(df_queried, colletion_name, property_name, index_name='Fecha'):
    """ Filters 'df_queried' by a 'collection_name' and 'property_name', from which it returns 'Datetime', 'Child', 'Value' fields. Converts 'Child' element into columns """
    dfOut = df_queried[(df_queried['Collection'] == colletion_name) & (df_queried['Property'] == property_name)]
    dfOut = dfOut[['Datetime', 'Child', 'Value']].set_index(['Datetime', 'Child'])['Value'].unstack()
    dfOut.index.name = index_name
    return dfOut


def get_parent_collection_property_from_dataframe(df_queried, colletion_name, property_name, parent_name, index_name='Fecha'):
    """ Filters 'df_queried' by a 'collection_name', 'parent_name' and 'property_name', from which it returns 'Datetime', 'Child', 'Value' fields. Converts 'Child' element into columns """
    dfOut = df_queried[
        (df_queried['Collection'] == colletion_name) &
        (df_queried['Property'] == property_name) &
        (df_queried['Parent'] == parent_name)
    ]
    dfOut = dfOut[['Datetime', 'Child', 'Value']].set_index(['Datetime', 'Child'])['Value'].unstack()
    dfOut.index.name = index_name
    return dfOut


def write_output_excel(ExcelNom, sheetname_data):
    """ Writes an Excel Workbook from the dictionary 'sheetname_data' of pandas.DataFrames """
    with pd.ExcelWriter(ExcelNom) as writer:
        for sheetname, df_data in sheetname_data.items():
            df_data.to_excel(writer, sheet_name=sheetname)
