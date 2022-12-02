# -*- coding: utf-8 -*- 
'''
Created on 28 nov. 2022

@author: Christian Luppi
'''
from datetime import datetime
from Logger import Logger
from os.path import join, exists
from os import remove, listdir, rename, makedirs
from time import strftime
import requests
import psycopg2

class Examen (object):

    def __init__ (self, logger):
        self.logger = logger
        self.archivoEntrada = None

    def cargarConfiguracion(self,configFile):
        self.logger.info("Cargando config: {}".format(configFile))
        self.config = {}
        
        with open(configFile, 'r') as config :
            for linea in config:
                linea = linea.strip()
                if linea == '' or linea[0:1] == '#':
                    continue
        
                partes = linea.split("=")
        
                clave = partes[0].strip()
                valor = partes[1].strip()
                
                self.config[clave] = valor
                
        self.logger.info("Config: {}".format(self.config))
        
        self.host = self.config["host"]
        self.dataBase = self.config["database"]
        self.user = self.config["user"]
        self.password = self.config["password"]
        self.port = self.config["port"]

    def conexionBd (self):
        conn = None
        try:
            
            #Establezco conexion con bd
            self.logger.info("Conectando a la Base de Datos PostgreSQL")
            conn = psycopg2.connect(database=self.dataBase, user=self.user, 
                    password=self.password, host=self.host, port= self.port)
            #conn.autocommit = True
            return conn

        except (OSError, IOError) as e:
            self.logger.error(f"Error al conectar a la BD: {e}")
            exit (1)


    def generarDirectorios(self,nombreFinal,fechaCreacionAñoMes):
        salidafullpath = f"../input/{nombreFinal}/{fechaCreacionAñoMes}"
        try:
            makedirs(salidafullpath)
        except(OSError, IOError) as e:
            self.logger.error(f"{e}")

    def obtenerInformacion(self, urls):

        try:
            for url in urls:
                archivo = requests.get(url)
                
                name = url.split("/")
                nombreFinal = name[-1].split("_")[0]

                fechaCreacion = datetime.now()
                fechaCreacionAñoMes = fechaCreacion.strftime("%Y-%B")

                self.generarDirectorios(nombreFinal,fechaCreacionAñoMes)

                fechaCreacionAñoMesDia = fechaCreacion.strftime("%d-%m-%Y")
               
                self.archivoEntrada = f"../input/{nombreFinal}/{fechaCreacionAñoMes}/{nombreFinal}-{fechaCreacionAñoMesDia}.csv"

                open(self.archivoEntrada, 'wb').write(archivo.content)
                
                self.insertarEnTabla()

        except (OSError, IOError) as e:
            self.logger.error(f"Error al abrir el archivo: {e}")
            exit (1)

    def insertarEnTabla(self):
        
        try:    
            with open(self.archivoEntrada, encoding="utf8") as fpIn:
                
                for line in fpIn:
                    #print(f'self.archivoEntrada: {self.archivoEntrada}')
                    if 'salas_cines' not in self.archivoEntrada:
                        print('Entrando acá')
                        linea = line.split(',')
                        cod_localidad = linea[0]
                        id_provincia = linea[1]
                        id_departamento = linea[2]
                        categoria = linea[3]
                        if 'Bibliotecas Populares'in categoria:
                            categoria = 'bibliotecas'
                        elif 'Espacios de Exhibición Patrimonial' in categoria:
                            categoria = 'museos'
                        provincia = linea[4]
                        localidad = linea[6]
                        nombre = linea[7]
                        domicilio = linea[8]
                        codigoPostal = linea[10]
                        telefono = None
                        mail = None
                        web = linea[11]

                        #Creación del cursor
                        conn = self.conexionBd()
                        cursor = conn.cursor()

                        #Sentencia para crear la Tabla
                        sql = '''INSERT INTO challenge (cod_localidad, id_provincia, id_departamento, categoría, 
                                    provincia, localidad, nombre, domicilio, codigo_postal, telefono, mail, web)
                                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

                        try:
                            #Borro la talba si existe
                            cursor.execute("DROP TABLE IF EXISTS challenge")
                            
                            #Creo la talba
                            cursor.execute(sql)
                            self.logger.info("Se creó correctamente la Tabla")
                            conn.commit
                        except (OSError, IOError) as e:
                            self.logger.error(f"Error al crear la Tabla: {e}")
                            exit (1)

                        #Cierro cursor BD
                        conn.close()

        except (OSError, IOError) as e:
            self.logger.error(f"Error al abrir el archivo: {e}")
            exit (1)

if __name__ == '__main__':
    logger = Logger("../log", "Examen", dateFormat="%Y%m", timeFormat="")
    logger.info("-----------------------------------------------------------------------------------")    
    logger.info("Ejecutando Examen: {}".format(strftime("%Y-%m-%d %H:%M:%S")))
    
    urls = ["https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv",
            "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv",
            "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/f7a8edb8-9208-41b0-8f19-d72811dcea97/download/salas_cine.csv"]

    archivoConfig = "configuracion.ini"

    prueba = Examen(logger)
    prueba.cargarConfiguracion(archivoConfig)
    prueba.obtenerInformacion(urls)