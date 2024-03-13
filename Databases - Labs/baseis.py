import mysql.connector

from pprint import pprint

mydb = mysql.connector.connect( host="localhost", user="root", passwd="!ggia3693")


cursor = mydb.cursor() 

    

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()


    for database in databases:
        print(database)
        





if (__name__== '__main__'):
    
    mydb = mysql.connector.connect(host="localhost",user="root",
                                   password="!ggia3693",
                                   database="testlab2DB",
                                   allow_local_infile=True)

    cursor = mydb.cursor()
    cursor.execute("SET GLOBAL local_infile=1")

    query = "LOAD DATA LOCAL INFILE 'studentCONVERTED.csv' INTO TABLE student FIELDS TERMINATED BY ';' \
    ENCLOSED BY '\"' LINES TERMINATED BY '\n'"
    cursor.execute(query)
    print("affected rows = {}".format(cursor.rowcount))

    load_query = "LOAD DATA LOCAL INFILE 'professorCONVERTED.csv' INTO TABLE professor FIELDS TERMINATED BY ';' \
    ENCLOSED BY '\"' LINES TERMINATED BY '\r\n'"
    cursor.execute(load_query)
    print("affected rows = {}".format(cursor.rowcount))

    loadquery = "LOAD DATA LOCAL INFILE 'advisedbyCONVERTED.csv' INTO TABLE advisedby FIELDS TERMINATED BY ';'  \
    LINES TERMINATED BY '\n'"
    cursor.execute(loadquery)
    print("affected rows = {}".format(cursor.rowcount))

    cursor.execute('COMMIT')
    

    #tables = ['student', 'professor', 'course', 'advisedby', 'taughtby']
    #for table in tables:
        #query = "SELECT * FROM %s"%table
        #cursor.execute(query)
        #print_results(query, cursor)
        #print(query)
        #print_no_results(cursor)


    
    #cursor.execute("SELECT MIN(student.yearInProgram), MAX(student.yearInProgram) FROM student AS S1, student AS S2")
    fetched = cursor.fetchall()
    pprint(fetched)
    

    

    



        
    mydb.close()



    








