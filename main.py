import urllib.request
import json
from decimal import Decimal
import psycopg2
import sys
from fastapi import FastAPI
app = FastAPI()

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

def Conexion():
    try:
        conn = psycopg2.connect(database="MetrobusCDMX",
                                user='postgres', password='123456', 
                                host='127.0.0.1', port='5432'
        )

        conn.autocommit = True
        cursor = conn.cursor()
        #print('Conexión Exitosa!!')
        return cursor
    except:
        e = sys.exc_info()[1]
        print(e.args[0])

def InsertaDatos():
    try:
        cursor = Conexion()
        response = urllib.request.urlopen('https://datos.cdmx.gob.mx/api/3/action/datastore_search?resource_id=ad360a0e-b42f-482c-af12-1fd72140032e&limit=500').read()
        jsonResponse = json.loads(response.decode('utf-8'))

        for child in jsonResponse['result']['records']:
            #print (child['_id'], child['id'], child['date_updated'], child['vehicle_id'], child['vehicle_label'], 
            #        child['vehicle_current_status'], child['position_latitude'], child['position_longitude'], child['geographic_point'], 
            #        child['position_speed'], child['position_odometer'], child['trip_schedule_relationship'], child['trip_id'], 
            #        child['trip_start_date'], child['trip_route_id'])
            cursor.execute("CALL MetrobusCDMX.InsertaDatos(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (child['_id'], child['id'], child['date_updated'], child['vehicle_id'], child['vehicle_label'], 
                    child['vehicle_current_status'], child['position_latitude'], child['position_longitude'], child['geographic_point'], 
                    child['position_speed'], child['position_odometer'], child['trip_schedule_relationship'], child['trip_id'], 
                    child['trip_start_date'], child['trip_route_id']))
        
        print('Datos Insertados correctamente!!')
        return
    except:
        print("Error inesperado:", sys.exc_info()[0])

def EliminaUnidades():
    try:
        cursor = Conexion()
        cursor.execute("TRUNCATE TABLE MetrobusCDMX.unidades;")
        print('Datos Eliminados Correctamente!!')
        return
    except:
        e = sys.exc_info()[1]
        print(e.args[0])

def InsertaAlcaldias():
    try:
        cursor = Conexion()
        response = urllib.request.urlopen('https://datos.cdmx.gob.mx/api/3/action/datastore_search?resource_id=e4a9b05f-c480-45fb-a62c-6d4e39c5180e&limit=25').read()
        jsonResponse = json.loads(response.decode('utf-8'))

        for child in jsonResponse['result']['records']:
            #print(child['_id'], child['id'], child['nomgeo'], child['cve_mun'], child['cve_ent'], child['cvegeo'], child['geo_point_2d'], child['municipio'])
            cursor.execute("CALL metrobuscdmx.insertaAlcaldias(%s, %s, %s, %s, %s, %s, %s, %s, %s);",(child['_id'], child['id'], child['nomgeo'], child['cve_mun'], child['cve_ent'], child['cvegeo'], child['geo_point_2d'], child['geo_shape'], child['municipio']))
        print('Datos Insertados correctamente!!')
        return
    except:
        e = sys.exc_info()[1]
        print(e.args[0])

@app.get("/consulta-unidades")
def ConsultaUnidades():
    try:
        cursor = Conexion()
        cursor.execute('SELECT id, vehicle_id, vehicle_label, vehicle_current_status, trip_id, trip_route_id FROM MetrobusCDMX.unidades;')
        datos = cursor.fetchall()

        arraylist_unidades = []
        for row in datos:
            registro = (row[0], row[1], row[2], row[3], row[4], row[5])
            arraylist_unidades .append(registro)

        json_unidades = json.dumps(arraylist_unidades, cls=JSONEncoder)
        cursor.close()
        return json_unidades
    except:
        e = sys.exc_info()[1]
        print(e.args[0])

@app.get("/consulta-alcaldias")
def ConsultaAlcaldias():
    try:
        cursor = Conexion()
        cursor.execute('SELECT id, nomgeo, cve_mun, cve_ent, cvegeo, geo_point_2d, municipio FROM MetrobusCDMX.alcaldias')
        datos = cursor.fetchall()

        arraylist_alcaldias = []
        for row in datos:
            registro = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
            arraylist_alcaldias.append(registro)
        
        json_alcaldias = json.dumps(arraylist_alcaldias, cls=JSONEncoder)

        cursor.close()
        return json_alcaldias
    except:
        e = sys.exc_info()[1]
        print(e.args[0])

@app.get("/consulta-unidad-id")
def ConsultaUbicacionXUnidad(id):
    try:
        cursor = Conexion()

        sql = 'SELECT id, vehicle_id, vehicle_label, vehicle_current_status, trip_id, trip_route_id FROM MetrobusCDMX.unidades WHERE vehicle_id = %(id)s;'
        cursor.execute(sql, {'id':id})
        datos = cursor.fetchall()

        arraylist_unidad = []
        for row in datos:
            registro = (row[0], row[1], row[2], row[3], row[4], row[5])
            arraylist_unidad.append(registro)
        
        json_unidades = json.dumps(arraylist_unidad, cls=JSONEncoder)

        cursor.close()
        return json_unidades
    except:
        e = sys.exc_info()[1]
        print(e.args[0])




#EliminaUnidades()

#InsertaDatos()

#ConsultaUnidades()

#ConsultaUbicacionXUnidad(1500)

#InsertaAlcaldias()

ConsultaAlcaldias()
