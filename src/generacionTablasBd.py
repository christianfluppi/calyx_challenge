# -*- coding: utf-8 -*- 
'''
Created on 28 nov. 2022

@author: Christian Luppi
'''

from Logger import Logger
from time import strftime
import psycopg2

class GeneracionTablas (object):

    def __init__ (self, logger):
        self.logger = logger

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
    
    def creacionTable(self):
        
        #Creación del cursor
        conn = self.conexionBd()
        cursor = conn.cursor()

        #Sentencia para crear la Tabla
        sql = '''CREATE TABLE challenge (
                    cod_localidad INTEGER NOT NULL,
                    id_provincia INTEGER NOT NULL,
                    id_departamento INTEGER NOT NULL,
                    categoría VARCHAR(20) NOT NULL,
                    provincia VARCHAR(100) NOT NULL,
                    localidad VARCHAR(200) NOT NULL,
                    nombre VARCHAR(50) NOT NULL,
                    domicilio VARCHAR(100) NOT NULL,
                    codigo_postal INTEGER NOT NULL,
                    telefono INTEGER,
                    mail VARCHAR(50),
                    web VARCHAR(100)
            )'''

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

if __name__ == '__main__':
    logger = Logger("../log", "GeneracionTablas", dateFormat="%Y%m", timeFormat="")
    logger.info("-----------------------------------------------------------------------------------")    
    logger.info("Ejecutando GeneracionTablas: {}".format(strftime("%Y-%m-%d %H:%M:%S")))

    archivoEntrada = 'configuracion.ini'

    generacionTabla = GeneracionTablas(logger)
    generacionTabla.cargarConfiguracion(archivoEntrada)
    generacionTabla.creacionTable()