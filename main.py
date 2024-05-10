from lector_reglas_repo import LectorReglasRepo
from variables import ruta_repo,ruta_reglas,file_name_tiporeglas,ruta_file_output,file_name_componentes,file_name_reglas

def main():
    
    lector = LectorReglasRepo()
      
    datos_componentes=lector._componentes(ruta_repo,ruta_file_output,file_name_componentes)
    listar_repositorios=lector._configuracion_reglas(ruta_repo,ruta_reglas,file_name_tiporeglas,ruta_file_output,file_name_reglas)
  
if __name__ == "__main__":
    main()