import os
import sys
#Agrega el directorio padre al path, en este caso src
sys.path.append("..")  
import unittest
from src.lector_reglas_repo import LectorReglasRepo

class TestLectorReglasRepo(unittest.TestCase):
    
    def test_ruta(self):
       # Definir datos de prueba dummy
        #Ruta de prueba
        directorio=os.path.dirname(os.path.realpath(__file__))
        #repositorio de prueba
        ruta_directorio=os.path.join(directorio,"test")
        #nombre de repositorio de prueba gldqkcogalpha
        ruta_repositorio=os.path.join(ruta_directorio,"gldqkcogalpha")
        #print(ruta)
        return ruta_directorio,ruta_repositorio

    def test_validacionesprevias(self):
                
        ruta_directorio, ruta_repositorio=self.test_ruta()
              
        #Validaciones para el procesamiento de datos componentes
        #Validamos archivos jenkisfile
        ruta_jenkinsfile=os.path.join(ruta_repositorio,"Jenkinsfile")
        if os.path.exists(ruta_jenkinsfile):
            print("Archivo Jenkinsfile se encuentra en la ruta, para obtener la versión.")
        else:
            print("¡ERROR! No se encontró el archivo Jenkinsfile en la ruta, no se puede obtener la versión.")
        
        #Validamos directorio configs
        ruta_configs=os.path.join(ruta_repositorio,"configs")
        if os.path.exists(ruta_configs):
            print("Directorio configs se encuentra en la ruta, para obtener los archivos de configuración.")
        else:
            print("¡ERROR! No se encontró el directorio configs en la ruta, no se puede obtener los archivos de configuración.")
        
        if len(os.listdir(ruta_configs))>0:
            print("Archivos conf se encuentran en la ruta para el procesamiento de los datos.")
        else:
            print("¡ERROR! No se encontraron archivos de conf, no se puede realizar el procesamiento de los datos.")
    
      
    def test_dataframes_componentesreglas(self):
           
       ruta_directorio, ruta_repositorio=self.test_ruta()
       
       # Instanciar LectorReglasRepo
       lector = LectorReglasRepo()
               
       # Validamos Dataframe de la función _componentes
       dataframe_componentes = lector._componentes(ruta_directorio)
           
       # Verificar si el dataframe resultante contiene datos
       if len(dataframe_componentes) > 0:
            print("El DataFrame de componentes tiene datos.")
       else:
            print("El DataFrame de componentes está vacío.")
        
       # Validamos Dataframe de la función _configuracion_reglas
       ruta_repo_configs=os.path.join(ruta_repositorio,"configs") 
       ruta_file_conf=os.path.join(ruta_repo_configs,"t_kcog_atm.conf")
       
       with open(ruta_file_conf, 'r') as archivo:
        contenido = archivo.read()
        if 'hammurabi' in contenido:
            print('La palabra hammurabi se encuentra en el archivo, se puede iniciar con el proceso de datos.')
        else:
            print('La palabra hammurabi no se encuentra en el archivo, no se puede iniciar con el proceso de los datos.')
            
        if 'rules' in contenido:
            print('La palabra rules se encuentra en el archivo, se puede iniciar con el proceso de datos de las reglas.')
        else:
            print('La palabra rule no se encuentra en el archivo, no se puede iniciar con el proceso de los datos de las reglas.')

       #Validamos Dataframe de reglas
       dataframe_reglas = lector._configuracion_reglas(ruta_directorio)
       
       # Verificar si el dataframe resultante contiene datos
       if len(dataframe_reglas) > 0:
            print("El DataFrame de reglas tiene datos.")
       else:
            print("El DataFrame de reglas está vacío.")
   
if __name__ == '__main__':
    unittest.main()
  
