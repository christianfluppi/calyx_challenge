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

class Examen (object):

    def __init__ (self, logger):
        self.logger = logger

    
    def generarDirectorios(self,nombreFinal,fechaCreacionAñoMes):
        salidafullpath = f"../input/{nombreFinal}/{fechaCreacionAñoMes}"
        try:
            makedirs(salidafullpath)
        except(OSError, IOError) as e:
            self.logger.error("Ya existen las carpetas")

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
               
                archivoEntrada = f"../input/{nombreFinal}/{fechaCreacionAñoMes}/{nombreFinal}-{fechaCreacionAñoMesDia}.csv"

                open(archivoEntrada, 'wb').write(archivo.content)

                self.unificarArchivos(archivoEntrada)

        except (OSError, IOError) as e:
            self.logger.error("Error al abrir el archivo")
            exit (1)

    def unificarArchivos(self,archivoEntrada):
        try:
            with open(archivoEntrada, 'wb') as fpIn:
                for line in fpIn:
                    linea = line.split(',')

        except (OSError, IOError) as e:
            self.logger.error("Error al abrir el archivo")
            exit (1)

if __name__ == '__main__':
    logger = Logger("../log", "Examen", dateFormat="%Y%m", timeFormat="")
    logger.info("-----------------------------------------------------------------------------------")    
    logger.info("Ejecutando Examen: {}".format(strftime("%Y-%m-%d %H:%M:%S")))
    
    urls = ["https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv",
            "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv",
            "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/f7a8edb8-9208-41b0-8f19-d72811dcea97/download/salas_cine.csv"]

    prueba = Examen(logger)
    prueba.obtenerInformacion(urls)
    