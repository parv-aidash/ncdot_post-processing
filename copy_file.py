import os
import shutil
import psycopg2 as ps
#s3 = boto3.resource('s3')
#copy_source = {
#    'Bucket': 'aidash-rsms-main',
#    'Key': 'ncdot/final_data/labelbox/shapefiles/lz/R1-TMX7317102405-000069-W_000001-front_point/R1-TMX7317102405-000069-W_000001-front_point.shp'
#}
#s3.meta.client.copy(copy_source, 'aidash-rsms-main', 'ncdot/final_data/labelbox/shapefiles/working/R1-TMX7317102405-000069-W_000001-front_point/R1-TMX7317102405-000069-W_000001-front_point.shp')
#os.chdir('/Users/parv.agarwal/Desktop/shapefiles/')
#shutil.rmtree('labelbox', ignore_errors=True)
#os.remove('labelbox')
host="localhost"
database="dot"
port="5435"
user="postgres"
password="VQaPo4AapgXbYlKb"

def connect_to_db(host,database,port,user,password):
    try:
        conn = ps.connect(
        host=host,
        database=database,
        port=port,
        user=user,
        password=password)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected')
        return conn

def delete_table(curr):
    delete_command = ('''Drop table ncdot."test_point1"''')
    curr.execute(delete_command)

conn = connect_to_db(host,database,port,user,password)
curr = conn.cursor()
delete_table(curr)
conn.commit()
curr.close()
conn.close()
