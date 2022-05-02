import urllib.request
import json
from decimal import Decimal
import psycopg2
import sys
from fastapi import FastAPI
app = FastAPI()

#Creación de la clase JSONEncoder para dar solución al error "Object of type Decimal is not JSON serializable"
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

#Metodo para la conexión a la BD de PostgreSQL
def Conexion():
    #inicia el manejo de errores
    try:
        #Creación de la cadena de conexión
        conn = psycopg2.connect(database="MetrobusCDMX",                #Nombre de la base de datos a la cual nos vamos a conectar
                                user='postgres', password='123456',     #Usuario y password para acceder a la BD
                                host='127.0.0.1', port='5432'           #Host (servidor) y puerto de acceso
        )

        #Se configuran las operaciones como Autocommit para evitar hacerlo manualmente
        conn.autocommit = True
        #Creación del cursor que servira para realizar las operaciones en la BD
        cursor = conn.cursor()
        #Se retorna el objeto cursor
        return cursor
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])

#Endpoint generado para la actualización de los datos de las unidades
@app.get("/actualiza-unidades")
#Metodo generado para insertar los datos consultados desde la API del Metrobus CDMX
def ActualizaUnidades():
     #inicia el manejo de errores
    try:
        #Inicialización de la variable cursor mandando a llamar el metodo Conexion
        cursor = Conexion()
        #Ejecución del metodo EliminaUnidades que limpia la tabla de unidades dentro de la BD
        EliminaUnidades()

        #Consultamos la API del Metrobus CDMX para obtener las Unidades
        response = urllib.request.urlopen('https://datos.cdmx.gob.mx/api/3/action/datastore_search?resource_id=ad360a0e-b42f-482c-af12-1fd72140032e&limit=500').read()
        #Almacenamos los datos en formato json
        jsonResponse = json.loads(response.decode('utf-8'))

        #for que nos sirve para recorrer cada "unidad" almacenada dentro del "tag" llamado "records", que es donde vienen los datos de las Unidades
        for child in jsonResponse['result']['records']:
            #Mandamos ejecutar el Store Procedure que inserta dentro de la tabla Unidades de la BD, pasando como parametros los datos obtenidos de la API
            cursor.execute("CALL MetrobusCDMX.InsertaDatos(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (child['_id'], child['id'], child['date_updated'], child['vehicle_id'], child['vehicle_label'], 
                    child['vehicle_current_status'], child['position_latitude'], child['position_longitude'], child['geographic_point'], 
                    child['position_speed'], child['position_odometer'], child['trip_schedule_relationship'], child['trip_id'], 
                    child['trip_start_date'], child['trip_route_id']))
        #Inicializamos el mensaje de ejecución exitosa
        mensaje = 'Datos Insertados correctamente!!'
        #Cerramos la conexión a la BD
        cursor.close()
        #Retornamos la variable "mensaje" a nuestro Endpoint
        return mensaje
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])

#Metodo generado para limpiar los datos de las unidades antes de realizar la carga (en teoría, la API actualiza los datos constantemente por lo que es necesario refrescar los datos)
def EliminaUnidades():
     #inicia el manejo de errores
    try:
        #Inicialización de la variable cursor mandando a llamar el metodo Conexion
        cursor = Conexion()
        cursor.execute("TRUNCATE TABLE MetrobusCDMX.unidades;")

        #Cerramos la conexión a la BD
        cursor.close()
        return
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])

#Metodo generado para la inserción inicial del catalogo de unidades
def InsertaAlcaldias():
     #inicia el manejo de errores
    try:
        #Inicialización de la variable cursor mandando a llamar el metodo Conexion
        cursor = Conexion()

        #Consultamos la API del Metrobus CDMX para obtener las Alcaldias
        response = urllib.request.urlopen('https://datos.cdmx.gob.mx/api/3/action/datastore_search?resource_id=e4a9b05f-c480-45fb-a62c-6d4e39c5180e&limit=25').read()
        #Almacenamos los datos en formato json
        jsonResponse = json.loads(response.decode('utf-8'))

        #for que nos sirve para recorrer cada "Alcaldia" almacenada dentro del "tag" llamado "records", que es donde vienen los datos de las Alcaldias
        for child in jsonResponse['result']['records']:
            #Mandamos ejecutar el Store Procedure que inserta dentro de la tabla Alcaldias de la BD, pasando como parametros los datos obtenidos de la API
            cursor.execute("CALL metrobuscdmx.insertaAlcaldias(%s, %s, %s, %s, %s, %s, %s, %s, %s);",(child['_id'], child['id'], child['nomgeo'], child['cve_mun'], child['cve_ent'], child['cvegeo'], child['geo_point_2d'], child['geo_shape'], child['municipio']))
        #Imprimimos el mensaje de ejecución exitosa
        print('Datos Insertados correctamente!!')
        #Cerramos la conexión a la BD
        cursor.close()
        #Retornamos
        return
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])

#Endpoint generado para la consulta de unidades
@app.get("/consulta-unidades")
#Metodo generado para consultar las unidades almacenadas en la BD
def ConsultaUnidades():
     #inicia el manejo de errores
    try:
        #Inicialización de la variable cursor mandando a llamar el metodo Conexion
        cursor = Conexion()
        #Enviamos el SELECT para que se ejecute en nuestro BD
        cursor.execute('SELECT id, vehicle_id, vehicle_label, vehicle_current_status, trip_id, trip_route_id FROM MetrobusCDMX.unidades;')
        #Obtenemos los datos devueltos por nuestra consulta
        datos = cursor.fetchall()

        #Declaramos un ArrayList vacio
        arraylist_unidades = []
        #for que nos sirve para recorrer los registros obtenidos en la consulta
        for row in datos:
            #Asignamos los valores obtenidos a la varaible "registro"
            registro = (row[0], row[1], row[2], row[3], row[4], row[5])
            #agregamos los datos a nuestro array list
            arraylist_unidades.append(registro)

        #Generamos el Json final utilizando el Arraylist y la clase JSONEncoder
        json_unidades = json.dumps(arraylist_unidades, cls=JSONEncoder)
        #Cerramos la conexión a la BD
        cursor.close()
        #Retornamos el Json final
        return json_unidades
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])

#Endpoint generado para la consulta de las alcaldias
@app.get("/consulta-alcaldias")
#Metodo generado para consultar las alcaldias existentes en la BD
def ConsultaAlcaldias():
     #inicia el manejo de errores
    try:
        #Inicialización de la variable cursor mandando a llamar el metodo Conexion
        cursor = Conexion()
        #Enviamos el SELECT para que se ejecute en nuestro BD
        cursor.execute('SELECT id, nomgeo, cve_mun, cve_ent, cvegeo, geo_point_2d, municipio FROM MetrobusCDMX.alcaldias')
        #Obtenemos los datos devueltos por nuestra consulta
        datos = cursor.fetchall()

        #Declaramos un ArrayList vacio
        arraylist_alcaldias = []
        #for que nos sirve para recorrer los registros obtenidos en la consulta
        for row in datos:
            #Asignamos los valores obtenidos a la varaible "registro"
            registro = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
            #agregamos los datos a nuestro array list
            arraylist_alcaldias.append(registro)
        
        #Generamos el Json final utilizando el Arraylist y la clase JSONEncoder
        json_alcaldias = json.dumps(arraylist_alcaldias, cls=JSONEncoder)
        #Cerramos la conexión a la BD
        cursor.close()
        #Retornamos el Json final
        return json_alcaldias
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])

#Endpoint generado para la consulta de unidades por su id
@app.get("/consulta-unidad-id")
#Metodo generado para consultar las unidades por su id
def ConsultaUbicacionXUnidad(id):
    #inicia el manejo de errores
    try:
        #Inicialización de la variable cursor mandando a llamar el metodo Conexion
        cursor = Conexion()
        #Declaramos el SELECT para que se ejecute en nuestro BD
        sql = 'SELECT id, vehicle_id, vehicle_label, vehicle_current_status, trip_id, trip_route_id FROM MetrobusCDMX.unidades WHERE vehicle_id = %(id)s;'
        #Ejecutamos la consulta junto con su parametro
        cursor.execute(sql, {'id':id})
        #Obtenemos los datos devueltos por nuestra consulta
        datos = cursor.fetchall()

        #Declaramos un ArrayList vacio
        arraylist_unidad = []
        #for que nos sirve para recorrer los registros obtenidos en la consulta
        for row in datos:
            #Asignamos los valores obtenidos a la varaible "registro"
            registro = (row[0], row[1], row[2], row[3], row[4], row[5])
            #agregamos los datos a nuestro array list
            arraylist_unidad.append(registro)
        
        #Generamos el Json final utilizando el Arraylist y la clase JSONEncoder
        json_unidades = json.dumps(arraylist_unidad, cls=JSONEncoder)
        #Cerramos la conexión a la BD
        cursor.close()
        #Retornamos el Json final
        return json_unidades
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])

#Endpoint generado para la consulta de las unidades que se encuentran en un alcaldia basados en su cve_mun
@app.get("/consulta-unidad-x-cve_alcaldia")
#Metodo generado para consultar las unidades que se encuenran en una alcaldia con base en su cve_mun
def ConsultaUnidadXAlcaldia(cve_mun):
    #inicia el manejo de errores
    try:
        #Inicialización de la variable cursor mandando a llamar el metodo Conexion
        cursor = Conexion()
        #Declaramos el SELECT para que se ejecute en nuestro BD
        sql = "SELECT A.nomgeo, U.vehicle_id "
        sql = sql +", ST_Contains("
        sql = sql +"   ST_GeomFromText(CONCAT('LINESTRING(',REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(A.geo_shape,'{\"type\": \"Polygon\", \"coordinates\": [[[',''),'], [','* '),']]]}',''),', ',' '),'*',','),')'))"
        sql = sql +"   ,ST_PointFromText(concat('POINT(',U.position_longitude,' ',U.position_latitude,')'))) exist "
        sql = sql +"FROM MetrobusCDMX.unidades U, "
        sql = sql +"MetrobusCDMX.alcaldias A "
        sql = sql +"WHERE A.cve_mun = %(cve_mun)s"
        sql = sql +" AND ST_Contains("
        sql = sql +"   ST_GeomFromText(CONCAT('LINESTRING(',REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(A.geo_shape,'{\"type\": \"Polygon\", \"coordinates\": [[[',''),'], [','* '),']]]}',''),', ',' '),'*',','),')'))"
        sql = sql +"   ,ST_PointFromText(concat('POINT(',U.position_longitude,' ',U.position_latitude,')')))"
        sql = sql +" ORDER BY 2 ASC;"

        #Ejecutamos la consulta junto con su parametro
        cursor.execute(sql, {'cve_mun':cve_mun})
        #Obtenemos los datos devueltos por nuestra consulta
        datos = cursor.fetchall()

        #Declaramos un ArrayList vacio
        arraylist_unidad = []
        #for que nos sirve para recorrer los registros obtenidos en la consulta
        for row in datos:
            #Asignamos los valores obtenidos a la varaible "registro"
            registro = (row[0], row[1], row[2])
            #agregamos los datos a nuestro array list
            arraylist_unidad.append(registro)
        
        #Generamos el Json final utilizando el Arraylist y la clase JSONEncoder
        json_unidades = json.dumps(arraylist_unidad, cls=JSONEncoder)
        #Cerramos la conexión a la BD
        cursor.close()
        #Retornamos el Json final
        return json_unidades
    #Manejo de excepciones
    except:
        e = sys.exc_info()[1]
        #Impresión del detalle del error
        print(e.args[0])
