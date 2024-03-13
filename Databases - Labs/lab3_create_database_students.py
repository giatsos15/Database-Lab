import mysql.connector


USER = "root"
DATABASE_NAME = "testLab_3DB"
psw = "!ggia3693"


def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall() ## it returns a list of all databases present
    ## showing one by one database
    for database in databases:
        print(database)

def create_tables(cursor):
    cursor.execute("CREATE TABLE student (\
        s_id INTEGER NOT NULL PRIMARY KEY, \
        inPhase VARCHAR(255), \
        yearInProgram INTEGER)")

    cursor.execute("CREATE TABLE professor (\
        p_id INTEGER NOT NULL PRIMARY KEY, \
        hasPosition VARCHAR(255))")

    cursor.execute("CREATE TABLE advisedby (\
        s_id INTEGER NOT NULL, \
        p_id INTEGER NOT NULL, \
        FOREIGN KEY (s_id) REFERENCES  student (s_id),\
        FOREIGN KEY (p_id) REFERENCES  professor (p_id),\
        PRIMARY KEY(s_id, p_id))")

    

def load_data(cursor):
    query = "LOAD DATA LOCAL INFILE 'data3/studentCONVERTED.csv' INTO TABLE student FIELDS TERMINATED BY ';' \
    ENCLOSED BY '\"' LINES TERMINATED BY '\n'"
    cursor.execute(query)
    print("affected rows = {}".format(cursor.rowcount))

    load_query = "LOAD DATA LOCAL INFILE 'data3/professorCONVERTED.csv' INTO TABLE professor FIELDS TERMINATED BY ';' \
    ENCLOSED BY '\"' LINES TERMINATED BY '\r\n'"
    cursor.execute(load_query)
    print("affected rows = {}".format(cursor.rowcount))

    load_query = "LOAD DATA LOCAL INFILE 'data3/advisedbyCONV.csv' INTO TABLE advisedby FIELDS TERMINATED BY ';'  \
    LINES TERMINATED BY '\n'"
    cursor.execute(load_query)
    print("affected rows = {}".format(cursor.rowcount))

    cursor.execute('COMMIT')


def print_no_results(cursor):
    results = cursor.fetchall()
    print("Number of results: "+str(len(results)))

def print_results(query, cursor):
    print(query)
    results = cursor.fetchall()
    for item in results:
        print(item)
    print("Number of results: "+str(len(results)))

    

if __name__ == '__main__':
    # 1. CREATE DATABASE
    mydb = mysql.connector.connect( host="localhost", user="root", passwd="!ggia3693")

    cursor = mydb.cursor()

    # cursor.execute("DROP DATABASE %s"%DATABASE_NAME)

    cursor.execute("CREATE DATABASE IF NOT EXISTS %s"%DATABASE_NAME)

    show_databases(cursor)

    mydb.close()

    # 2. LOAD DATA

    mydb    = mysql.connector.connect( host="localhost", user="root", passwd="!ggia3693", database="testlab_3db", allow_local_infile=True)

    cursor = mydb.cursor()

    cursor.execute("SET GLOBAL local_infile=1")

    create_tables(cursor)

    load_data(cursor)

    # # check if data has been loaded
    tables = ['student', 'professor', 'course', 'advisedby', 'taughtby']
    for table in tables:
        query = "SELECT * FROM %s"%table
        cursor.execute(query)
        print_results(query, cursor)
        print(query)
        print_no_results(cursor)


    mydb.close()
