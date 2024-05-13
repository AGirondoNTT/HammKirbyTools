import os
import re
import json
import pyhocon
from pyhocon import ConfigFactory, exceptions
from pyhocon.exceptions import ConfigMissingException
import pandas as pd


class LectorReglasRepo:

    def _limpiar_str(self, cadena):
        #Obtengo caracteres alfanumericos
        return re.sub(r'[^a-zA-Z0-9_]', '', cadena)
    
    
    def _parsear_input_output(self, valor_input):
        # Esta función es para los tipo de table, obtengo las capas de datos staging, raw, master
        #Pasar de [ConfigValues: [ConfigSubstitution: MASTERSCHEMA],[ConfigQuotedString: .t_krea_re_dev_deg_inspections]] -> master
        #capa = self._limpiar_str(valor_input.split(",")[0].split(":")[2]).lower()#[:-6]
        val_capa = self._limpiar_str(valor_input.split(",")[0].split(":")[2]).lower()#[:-6]
        
        if val_capa.find("staging")==0 or val_capa.find("raw")==0  or val_capa.find("master")==0:
            capa=val_capa[:-6]
        else:
            capa=""
        # Pasar de [ConfigValues: [ConfigSubstitution: MASTERSCHEMA],[ConfigQuotedString: .t_krea_re_dev_deg_inspections]] -> t_krea_re_dev_deg_inspections
        tabla = self._limpiar_str(valor_input.split(",")[1].split(":")[1])
        return capa, tabla
    
  
    def _obtener_version_jenkinsfile(self, ruta_archivo):
        #Obtenemos la versión de los archivos jenkinsfiles
        with open(ruta_archivo, 'r') as archivo:
            contenido = archivo.read()

            # Buscar la línea que contiene 'env.VERSION'
            for linea in contenido.split('\n'):
                if 'env.VERSION' in linea:
                    partes = linea.split('=')
                    if len(partes) == 2:
                        # Tomar la parte derecha y quitar comillas
                        version = partes[1].strip().strip('"')
                        return version

    def _obtener_archivos_conf(self,local_repo_path):
        #Creamos una variable lista confs
        confs = []
        for path, dirs, files in os.walk(local_repo_path):
            for filename in files:
                if filename.endswith(".conf"):
                    confs.append(os.path.join(path, filename))

        return confs

    def _obtener_tipo_conf(self, ruta_archivo):
        #print(ruta_archivo)
        with open(ruta_archivo, 'r') as archivo:
            for linea in archivo:
                palabras = linea.split()
                if palabras:
                    palabra_inicial = palabras[0]
                    if palabra_inicial in ["hammurabi", "kirby","ConfigLog"]:
                        return palabra_inicial


    def _obtener_valor_input(self, ruta_archivo):
                    
            config = ConfigFactory.parse_file(ruta_archivo, resolve=False)
            #Obtengo los tipos de configuración kirby,hammurabi,histable
            tipo_conf = self._obtener_tipo_conf(ruta_archivo)
            capa = ""
            tabla = ""
            if(tipo_conf=="kirby"):
                tipo_input = config.get(tipo_conf).get("output").get("type")
                
                if (tipo_input == "parquet"):
                    valor_input = config.get(tipo_conf).get("output").get("path")
                    valor_input_split = valor_input.split("/")
                    capa = valor_input_split[2]
                    var_uuaa=valor_input_split[3]
                                        
                    if (valor_input_split[-1]==""):
                        tabla = valor_input_split[-2]
                    else:
                        tabla = valor_input_split[-1]
                    
                    return tipo_conf,capa,tabla,var_uuaa
                
                elif (tipo_input == "table"):
                    valor_input = str(config.get(tipo_conf).get("output").get("table"))
                    resultado = self._parsear_input_output(valor_input)
                    var_uuaa_table1=resultado[1].split("_")
                    var_uuaa_table=var_uuaa_table1[1]
                    capa, tabla=resultado
                    
                    return tipo_conf,capa,tabla,var_uuaa_table
                    
            if(tipo_conf=="hammurabi"):
                tipo_input = config.get(tipo_conf).get("input").get("type")
                if (tipo_input == "parquet"):
                    valor_input = config.get(tipo_conf).get("input").get("paths")[0]
                    valor_input_split = valor_input.split("/")
                    capa = valor_input_split[2]
                    var_uuaa=valor_input_split[3]
                    if (valor_input_split[-1]==""):
                        tabla = valor_input_split[-2]
                    else:
                        tabla = valor_input_split[-1]
                    
                    return tipo_conf,capa,tabla,var_uuaa
                
                elif (tipo_input == "table"):
                    valor_input = str(config.get(tipo_conf).get("input").get("tables"))
                    resultado = self._parsear_input_output(valor_input)
                    var_uuaa_table1=resultado[1].split("_")
                    var_uuaa_table=var_uuaa_table1[1]
                    capa, tabla=resultado
                    
                    return tipo_conf,capa,tabla,var_uuaa_table

            if(tipo_conf=="ConfigLog"):
                valor_input = str(config.get(tipo_conf).get("hisTable").get("tablePath"))
                valor_input_split = valor_input.split("/")
                capa = valor_input_split[2]
                var_uuaa = valor_input_split[3]
                if (valor_input_split[-1]==""):
                    tabla = valor_input_split[-2]
                else:
                    tabla = valor_input_split[-1]
            else:
                valor_input = "Type no contemplado"
            return tipo_conf,capa,tabla,var_uuaa

   
    def _leer_catalogo_reglas(self,ruta_archivo):
        #Se lee el archivo tipos de regla
        with open(ruta_archivo, 'r') as archivo:
            catalogo = json.load(archivo)
            
        return catalogo

   
    def _obtener_campo_aplicado(self, ruta_conf, regla):
        #Se creo una variable lista
        columnas=[]
        try:
            columnas=regla.get("config").get("column")
            return columnas
            
        except exceptions.ConfigMissingException:
            try:
                columnas=regla.get("config").get("columns")
                return columnas
            except exceptions.ConfigMissingException:
                return columnas

    def _componentes(self, local_repo_path):
        
        datos_repos = []
        archivo_repo=os.listdir(local_repo_path)
        
        for repositorios in archivo_repo:
            #Obtengo el nombre de los repositorios
            nombre_repos=os.path.basename(repositorios)
            var_rutacompleta=os.path.join(local_repo_path,repositorios)
            ruta_modi2=var_rutacompleta.replace("\\", "/")
            rutas_confs=self._obtener_archivos_conf(ruta_modi2)
            #Obtengo los archivos jenkins
            archivos_jenk=os.listdir(ruta_modi2)

            #Se crea inicialmente un diccionario por las diferentes longitudes de los repos
            #En este diccionario obtengo nom y ver
            dato_repo={'nombre repo':nombre_repos} 
            
            #creamos las listas
            confs =   []
            tipos =   []
            capas =   []
            tablas =  []
            uuaa= []
            nombres_repos=[]
            version=[]

            for conf in rutas_confs:
                vconf_ruta_mod=conf.replace("\\", "/").strip()
                confs.append(os.path.basename(vconf_ruta_mod))
                #tipos.append(self._obtener_tipo_conf(vconf_ruta_mod))
                
                tipo_input, capa_input, tabla_input,uuaa_input = self._obtener_valor_input(vconf_ruta_mod)

                tipos.append(tipo_input)
                capas.append(capa_input)
                tablas.append(tabla_input)
                uuaa.append(uuaa_input)
            
            dato_repo['nombre componente']=confs
            dato_repo['tipo']=tipos
            dato_repo['capa']= capas
            dato_repo['tabla']= tablas
            dato_repo['uuaa']= uuaa
                        
            #Agrego la validación de la versión 
            if "Jenkinsfile" in archivos_jenk:
               ruta_jenkinsfile = os.path.join(ruta_modi2, "Jenkinsfile")
               ruta_jenkinsfile2=ruta_jenkinsfile.replace("\\", "/")
               val_version=self._obtener_version_jenkinsfile(ruta_jenkinsfile2)
               if val_version is not None:
                   dato_repo['version']=val_version
               else:
                   dato_repo['version']="Not version"
            #Agregar todos los datos a la lista
            datos_repos.append(dato_repo)
        
        #Creamos un dataframe a partir de la recopilación de los datos de los repositorios
        dataframe_componentes = pd.concat([pd.DataFrame(datos) for datos in datos_repos], ignore_index=True)
        #print(dataframe_componentes)   
        return dataframe_componentes
    
    #Obtener la lista de repositorio, ruta de los archivos de configuración,
    #nombre del archivo de configuración, nombre de la capa (Master, Staging,etc)
    #nombre de la tabla
    def _lista_repositorio(self, local_repo_path):
        datos_repositorio=[]
        listar_repo=os.listdir(local_repo_path)
        for repositorios in listar_repo:
            nom_repositorios=repositorios
            rut_completa_repo=os.path.join(local_repo_path,nom_repositorios).replace("\\","/")
            rut_archivos_confs=self._obtener_archivos_conf(rut_completa_repo)
            for archivos_confs in rut_archivos_confs:
                rut_archivos_conf_mod=archivos_confs.replace("\\", "/").strip()
                nom_archivos_conf=os.path.basename(rut_archivos_conf_mod)
                tipo, capa, tabla,uuaa = self._obtener_valor_input(rut_archivos_conf_mod)
                #En este caso para obtener las configuraciones de las reglas 
                #obtengo el tipo de configuración Hammurabi
                tipo_configuracion=self._obtener_tipo_conf(rut_archivos_conf_mod)
                
                if tipo_configuracion=="hammurabi":
                    #datos del dataframe
                    datos={
                            'REPOSITORIO':nom_repositorios,
                            'RUTA':rut_archivos_conf_mod,
                            'HAMMURABI':tipo_configuracion,
                            'NOMBRE DE ARCHIVO':nom_archivos_conf,
                            'CAPA':capa,
                            'ID OBJETO':tabla,
                    }
                    datos_repositorio.append(datos)
            
            dataframe = pd.DataFrame(datos_repositorio)
            
        return dataframe

    #Obtengo el dataframe creado en la función anterior
    #para complementarlo con información de las reglas
    def _configuracion_reglas(self,local_repo_path):
        
        datos_conf_reglas=[]
        df1=self._lista_repositorio(local_repo_path)
        #Obtengo la ruta del archivo de configuración desde el df1
        ruta_file_conf=df1['RUTA']
        #print(tipo_configuracion)
        for files_configuraciones in ruta_file_conf:
           config = ConfigFactory.parse_file(files_configuraciones, resolve=False)
           #Obtenemos información acerca de las reglas
           config_reglas = config.get("hammurabi").get("rules")
           #Realizamos un for para recorrer todas las reglas
           for reglas in config_reglas:
               #Obtengo la clase
               clase = reglas.get("class")
               #Obtener los tipos y subtipos de reglas
               #ruta_rule=os.path.join(ruta_reglas,file_name_tiporeglas).replace("\\", "/").strip()
               catalogo_reglas = self._leer_catalogo_reglas("./tipos_de_reglas.json")
               sub_tipo=catalogo_reglas[clase].replace(',','.')
               tipo=sub_tipo[0]
               
               #Obtengo Id de la regla MVP o Dominio
               id_reglas=reglas.get("config").get("id")
               #Obtengo el campo columns
               id_columns=self._obtener_campo_aplicado(files_configuraciones, reglas)
               #Obtengo el campo critical
               var_critical=reglas.get("config").get("isCritical", "")
               #Obtengo el campo acceptanceMin
               var_acceptanceMin = reglas.get("config").get("acceptanceMin", "")
               #Obtengo el campo withRefusals
               var_withRefusals= reglas.get("config").get("withRefusals", "")
               #Obtengo el campo treatEmptyValuesAsNulls
               var_treatEmptyValuesAsNulls=reglas.get("config").get("treatEmptyValuesAsNulls", "")
               #Obtengo el campo format
               var_format=reglas.get("config").get("format", "")
               #Obtengo el campo columns
               var_columns=reglas.get("config").get("columns", "")
               #Obtengo el campo lowerBound
               var_lowerBound=reglas.get("config").get("lowerBound", "")
               #Obtengo el campo upperBound
               var_upperBound=reglas.get("config").get("upperBound", "")
               #Obtengo el campo dataValuesSubset
               var_dataValuesSubset=reglas.get("config").get("dataValuesSubset", "")

               #datos del dataframe
               datos_reglas={
                    'RUTA':files_configuraciones,
                    'CLASE':clase,
                    'PRINCIPIO DE CALIDAD':tipo,
                    'TIPO DE REGLA':sub_tipo,
                    'ID FUNCIONAL':id_reglas,
                    'ID CAMPO':id_columns,
                    'REGLA BLOQUEANTE':var_critical,
                    'PORCENTAJE MINIMO DE ACEPTACION':var_acceptanceMin,
                    'TIPO DE MUESTRA DE RESULTADOS KOS':var_withRefusals,
                    'TRATAMIENTO DE VACIOS COMO NULOS':var_treatEmptyValuesAsNulls,
                    'FORMATO':var_format,
                    'CAMPOS CLAVE DEL OBJETO':var_columns,
                    'VARIACION MINIMA RELATIVA DE REGISTROS':var_lowerBound,
                    'VARIACION MAXIMA RELATIVA DE REGISTROS':var_upperBound,
                    'CONDICION DE FILTRADO':var_dataValuesSubset
                }
               datos_conf_reglas.append(datos_reglas)
        df2 = pd.DataFrame(datos_conf_reglas)

        #Realizar merge a los DataFrames
        dataframe_reglas = pd.merge(df1, df2, on='RUTA', how='inner')
        #print(dataframe_reglas)
        return dataframe_reglas
       
       

            






           

       
           

        
     

   



        

            
        
      
    

       

       
        
        

                    
                
                
                    
                        
                       
        
        

                    

        
      
                    


            
        
       
    
            