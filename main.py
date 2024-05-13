import os
from lector_reglas_repo import LectorReglasRepo
from variables import ruta_repo,ruta_file_output,file_name_componentes,file_name_reglas

def main():
    
    lector = LectorReglasRepo()
      
    datos_componentes=lector._componentes(ruta_repo)
    listar_repositorios=lector._configuracion_reglas(ruta_repo)
    
    #Exportamos los df formato csv
    #componentes
    var_ruta_componentes=os.path.join(ruta_file_output,file_name_componentes)
    var_ruta_componentes=var_ruta_componentes.replace("\\", "/")
    datos_componentes.to_csv(var_ruta_componentes, index=False)
    #reglas
    var_ruta_reglas=os.path.join(ruta_file_output,file_name_reglas)
    var_ruta_reglas=var_ruta_reglas.replace("\\", "/")
    listar_repositorios.to_csv(var_ruta_reglas, index=False)

if __name__ == "__main__":
    main()