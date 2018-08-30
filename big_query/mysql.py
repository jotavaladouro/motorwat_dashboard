import MySQLdb
import pandas as pd
from env import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

def connect_mysql():
    """
    Connect to a mysl BBDD
    :return: A mysql connection
    """
    datos = [DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT]
    lconn = MySQLdb.connect(*datos)
    return lconn

def close_mysql(lmysql_conn):
    """
    Close MySQLdb connection object
    :param lmysql_conn: MySQLdb connection object
    :return:
    """
    lmysql_conn.close()

def get_sql_query(day_load, lindex):
    """
    Return a string quety to get data from a lane and from a list of days.
    The Query colect info about cars with spanish obu only
    :param day_load .- Day to get from mysql
    :param lindex : Get only data higher than lindex
    :return: query string
    """

    sz = "select N_Mensaxe_C, N_Estacion_C, N_Via_C, D_Data_C,\
                T_Hora_C, Sz_Chave_C, N_Orixen_X, N_Destino_X,\
                N_Pago_X, N_Obu_Validez_In, N_Obu_Pago, N_Obu_Estacion,\
                D_Obu_Data, T_Obu_Time, N_Obu_Via_Entrada, indice\n\
        from peaje.tb_mensaxes_in_transitos\n \
        where N_Estacion_C = 6   and N_Via_C < 20 and N_Avance_X = 0  and\
        D_Data_C=\"" + day_load + "\" and indice>" + str(lindex) + " order by T_Hora_C"

    return sz

def get_data(lmysql_conn, day_load, index):
    # type: (object, str, int) -> Optional[pd.DataFrame]
    """
    Get data from mysql database
    :param lmysql_conn: Mysql connection
    :param day_load: day to get data
    :param index: last index read, get data older than index
    :return: None or a dataframe with new data
    """
    sz_sql = get_sql_query(day_load, index)
    cursor = lmysql_conn.cursor()
    cursor.execute(sz_sql)
    lst = list(cursor.fetchall())
    cursor.close()
    if len(lst) > 0:
        dataframe_result = pd.DataFrame(lst)
        return dataframe_result
    else:
        return None


