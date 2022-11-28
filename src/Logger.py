'''
Created on 8/7/2015

@author: gvillada
'''

import logging
from datetime import datetime
from os.path import join, exists


class Logger(object):
    #Tipos de salida del log
    SCREEN = 1
    LOG    = 2
    FULL   = 3
    
    lognameBase = ""
    logname = ""
    logger = None 

    def info(self, mensaje, tipoLog = FULL):
        """Loguea en el log y/o por pantalla"""
        
        if tipoLog in (self.FULL, self.SCREEN):
            print(mensaje)
    
        if tipoLog in (self.FULL, self.LOG):
            logging.info(mensaje)


    def error(self, mensaje, tipoLog = FULL):
        """Loguea en el log y/o por pantalla"""
        
        if tipoLog in (self.FULL, self.SCREEN):
            print(mensaje)
    
        if tipoLog in (self.FULL, self.LOG):
            logging.error(mensaje)


    def cerrarLogs(self):
        handlers = self.logger.handlers[:]
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)



    def __init__(self, logDir, logFileBase, dateFormat="%Y%m%d", timeFormat="%H%M%S"):
        
        if not exists(logDir) :
            print("Error, directorio no existe: {}".format(logDir))
            exit(-10)
        
        self.lognameBase = logFileBase
        
        if not self.lognameBase.endswith(".") :
            self.lognameBase += "."
            
        self.lognameBase += datetime.now().date().strftime(dateFormat) + "-" + datetime.now().time().strftime(timeFormat) + ".log"
        
        
        self.logname = join(logDir, self.lognameBase)
        
        try:
        
            logging.basicConfig(filename=self.logname,format='%(levelname)s:%(asctime)s:%(message)s',datefmt='%d/%m/%Y %I:%M:%S',level=logging.INFO)
            self.logger = logging.getLogger()

            #logger("Iniciando Script Version {}".format(version_script), LOGGER_OUT_FULL)
            #logger("Usando fecha: {}".format(fecha.strftime("%Y-%m-%d")), LOGGER_OUT_FULL)

        except Exception as e:

            logging.exception(e)
            print("")
            print("ERROR: \"{} -> {}\".PARA MAS INFO VER EL LOG ({})".format(str(type(e))[7:-2],str(e), self.logname))
            exit(-11)

        
        
        