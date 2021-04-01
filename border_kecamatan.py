#get data polygon kecamatan  
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import json

def insert_border_kecamatan():
    f = open('batas_kec_BKP.json',) 
    #f = open('batas_kec_test.json',)
    data = json.load(f)
    rows = 1
    if(rows == 1):
        for fitur in data['features']:
            datasrc = ""
            type = f"{fitur['geometry']['type']}"
            #polygon
            if type == 'Polygon':
                for coo in fitur['geometry']['coordinates'][0]:
                    datasrc += str(coo[0]) +' '+ str(coo[1]) + ','
            #multipolygon
            if type == 'MultiPolygon':
                for i in range(len(fitur['geometry']['coordinates'])):
                    datasrc += '('
                    for coo in fitur['geometry']['coordinates'][i][0]:
                        datasrc += str(coo[0]) +' '+ str(coo[1]) + ','
                    datasrc = datasrc[:-1]
                    datasrc += '),'

            datasrc = datasrc[:-1]

            if type == 'Polygon':
                datasrc = type+'((' + datasrc + '))'
            
            if type == 'MultiPolygon':
                datasrc = type+'((' + datasrc + '))'

            #header
            OBJECTID = f"{fitur['properties']['OBJECTID']}"
            STATISTIK = f"{fitur['properties']['STATISTIK']}"
            KECAMATAN = f"{fitur['properties']['KECAMATAN']}"
            KABUPATEN = f"{fitur['properties']['KABUPATEN']}"
            PROPINSI = f"{fitur['properties']['PROPINSI']}"
            LUAS_KM2 = f"{fitur['properties']['LUAS_KM2']}"
            DATA = f"{fitur['properties']['DATA']}"
            SHAPE_area = f"{fitur['properties']['SHAPE_area']}"
            SHAPE_len = f"{fitur['properties']['SHAPE_len']}"

            SHAPE_area = SHAPE_area.replace("e", "")
            SHAPE_len  = SHAPE_len.replace("e", "")

            #validasi
            if not STATISTIK:
                STATISTIK = 'null'
            if not LUAS_KM2:
                LUAS_KM2 = 'null'
            if not SHAPE_area:
                SHAPE_area = 'null'
            if not SHAPE_len:
                SHAPE_len = 'null'

            try:
                sqlEngine = create_engine('mysql+pymysql://root:@localhost:3306/jarvis_asset', pool_recycle=3600)
                dbConnection = sqlEngine.connect()
                q = "INSERT INTO `border_kecamatan` VALUES("+str(OBJECTID)+","+str(STATISTIK)+",'"+str(KECAMATAN)+"','"+str(KABUPATEN)+"','"+str(PROPINSI)+"',"+str(LUAS_KM2)+",'"+str(DATA)+"',"+str(SHAPE_area)+","+str(SHAPE_len)+",'"+str(datasrc)+"'); "
                #print(q)
                try :
                    pd.read_sql(q,sqlEngine)
                except Exception as e:
                    print(e)

                dbConnection.close()
                sqlEngine.dispose()
            except Exception as e:
                print(e)
            rows=rows+1

    f.close()

insert_border_kecamatan()