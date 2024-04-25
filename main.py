import json
import os
from lector_reglas_repo import LectorReglasRepo

def variables(archivo_variables):
        
    with open (archivo_variables) as var:
        variables=json.load(var)
    
    v_ruta_repo=variables['ruta_repo']
    v_ruta_file_output=variables['ruta_file_output']
    v_file_name_componentes=variables['file_name_componentes']
    v_file_name_reglas=variables['file_name_reglas']
    return v_ruta_repo,v_ruta_file_output,v_file_name_componentes,v_file_name_reglas

def main():
    
    lector = LectorReglasRepo()
    
    ruta_var_json="C:/Users/prengifm/OneDrive - NTT DATA EMEAL/Documentos/Proyecto_Ficheros_python/src/variables.json"
    ruta_repo,ruta_file, file_name_componentes, file_name_reglas=variables(ruta_var_json)
    

    datos_componentes=lector._componentes(ruta_repo,ruta_file,file_name_componentes)
    reglas=lector._reglas(ruta_repo,ruta_file,file_name_reglas)
  
if __name__ == "__main__":
    main()