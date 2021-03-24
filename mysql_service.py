import pymysql.cursors
import pandas as pd

# Connect to the database
host='localhost'
db_user='root'
password='1234'
db='xm_data'
charset='utf8'
cursorclass=pymysql.cursors.DictCursor

def insert_day(date, titles, tables):
    """
    param: User object
    returns the MySQL error handle by the try-except senteces
    """
   
    connection = pymysql.connect(host=host,
                             user=db_user,
                             password=password,
                             db=db,
                             charset=charset,
                             cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            e = 'none'
            # Create a new record
            
            sql = '''INSERT INTO historical_data_2 (date, titles, tables) VALUES ("%s","%s","%s")'''
    
            cursor.execute(sql, (str(date), titles, tables))
            

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

    except Exception as ex:        
        print(ex.args[0]) 
        e = ex.args[0]
    finally:
        connection.close()
        return e

def get_days():
    """
    param: username
    returns User instance with user data, the MySQL error handle by the try-except senteces
    """
    
    connection = pymysql.connect(host=host,
                             user=db_user,
                             password=password,
                             db=db,
                             charset=charset,
                             cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            
            sql = "SELECT date, titles, tables FROM historical_data_2"
            cursor.execute(sql)
            
            result = cursor.fetchall()
            print(type(result))
                 
    except Exception as ex:        
        print(ex.args[0]) 
    finally:
        connection.close()
        return  list(result)