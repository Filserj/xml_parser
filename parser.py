from lxml import etree
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import datetime


conn_str = """user='fias_user' dbname = 'fias' password='fias_user' host='localhost' port='5432'"""
connection = psycopg2.connect(conn_str)
##alchemyEngine = create_engine('postgresql+psycopg2://fias_user:fias_user@localhost:5432/fias')
##dbConnection = alchemyEngine.connect()
file = 'h:/fias_xml/AS_ROOM_20210803_0a80f689-d4c1-4d78-babe-98028296ca58.XML'

def parseXML(xmlFile):
    dfls = []
    context = etree.iterparse(xmlFile)
    attrlist = []
    rownum = 0
    for action, elem in context:
        attrlist = []
        vallist = []
        for i in elem.attrib:
            attrlist.append(i)
        for i in elem.values():
            vallist.append(i)
        d_row = dict(zip(attrlist, vallist))
##        row = pd.Series(data=vallist, index=attrlist)
##        print(row)
##        dfls.append(row)
        insert_query = 'insert into fias.as_room ("'+'","'.join(attrlist)+'") values ('+'%('+')s, %('.join(attrlist)+')s )'
        cursor = connection.cursor()
        cursor.execute(insert_query, d_row)
        rownum+=1
        if rownum % 100000 == 0:
            print('[', datetime.datetime.now(), '] fias.as_room commit, row = ', str(rownum))
            connection.commit()
##            return dfls
        elem.clear()
    cursor.close()
    connection.commit()
    connection.close()    

            
##file = r'g:\Users\Serega\Desktop\fias_delta_xml\AS_ROOM_20210719_453f19c4-aba5-4f68-b20e-246af201c3cb.XML' 
print('[', datetime.datetime.now(), '] Process started')
##df = pd.DataFrame(parseXML(file))
##print(df.head(3))
parseXML(file)
##print('[', datetime.datetime.now(), '] DataFrame Ready, length = ', len(df))
##df.to_sql(name='as_room', schema='fias', con=dbConnection, if_exists='append', index=False, chunksize=10000, method='multi')
##data = list(dbConnection.execute('select * from fias.as_room limit 5'))
##print('[', datetime.datetime.now(), '] Process Finished \n Data: \n', data[:5])
##dbConnection.close()
input('Press Enter to continue')
