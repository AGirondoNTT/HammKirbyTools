import os
import re
import json
import pyhocon
from pyhocon import ConfigFactory, exceptions, HOCONConverter
from pyhocon.exceptions import ConfigMissingException
import pandas as pd


class LectorReglasRepo:

    def _limpiar_str(self, cadena):
        return re.sub(r'[^a-zA-Z0-9_]', '', cadena)#Obtengo caracteres alfanumericos
    
    def _parsear_input_output(self, valor_input):
        #Pasar de [ConfigValues: [ConfigSubstitution: MASTERSCHEMA],[ConfigQuotedString: .t_krea_re_dev_deg_inspections]] -> master
        #Obtengo  staging, raw, master 
        #capa = self._limpiar_str(valor_input.split(",")[0].split(":")[2]).lower()#[:-6]
        val_capa = self._limpiar_str(valor_input.split(",")[0].split(":")[2]).lower()#[:-6]
        
        if val_capa.find("staging")==0 or val_capa.find("raw")==0  or val_capa.find("master")==0:
            capa=val_capa[:-6]
            #print(capa)
        else:
            capa=""
            #print(capa)
        # Pasar de [ConfigValues: [ConfigSubstitution: MASTERSCHEMA],[ConfigQuotedString: .t_krea_re_dev_deg_inspections]] -> t_krea_re_dev_deg_inspections
        tabla = self._limpiar_str(valor_input.split(",")[1].split(":")[1])
                
        return capa, tabla
    
    def _obtener_version_jenkinsfile(self, ruta_archivo):
        with open(ruta_archivo, 'r') as archivo:
            contenido = archivo.read()

            # Buscar la línea que contiene 'env.VERSION'
            for linea in contenido.split('\n'):
                if 'env.VERSION' in linea:
                    partes = linea.split('=')
                    if len(partes) == 2:
                        # Coger la parte derecha y quitar comillas
                        version = partes[1].strip().strip('"')
                        return version

    def _obtener_archivos_conf(self,local_repo_path):#modificado
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


    def _obtener_valor_input(self, ruta_archivo):#Modificado
                    
            config = ConfigFactory.parse_file(ruta_archivo, resolve=False)
            tipo_conf = self._obtener_tipo_conf(ruta_archivo)#kirby,hammurabi,histable
            #print(config)
            capa = ""
            tabla = ""
            if(tipo_conf=="kirby"):
                tipo_input = config.get(tipo_conf).get("output").get("type")
                #print(tipo_input)
                if (tipo_input == "parquet"):
                    valor_input = config.get(tipo_conf).get("output").get("path")#[0]
                    valor_input_split = valor_input.split("/")
                    capa = valor_input_split[2]
                    var_uuaa=valor_input_split[3]
                                        
                    if (valor_input_split[-1]==""):
                        tabla = valor_input_split[-2]
                    else:
                        tabla = valor_input_split[-1]
                    #print(valor_input,capa,tabla)
                    return tipo_conf,capa,tabla,var_uuaa
                
                elif (tipo_input == "table"):
                    valor_input = str(config.get(tipo_conf).get("output").get("table"))
                    resultado = self._parsear_input_output(valor_input)
                    var_uuaa_table1=resultado[1].split("_")
                    var_uuaa_table=var_uuaa_table1[1]
                    capa, tabla=resultado
                    #print(var_uuaa_table)
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
                    #print(var_uuaa)
                    return tipo_conf,capa,tabla,var_uuaa
                
                elif (tipo_input == "table"):
                    valor_input = str(config.get(tipo_conf).get("input").get("tables"))
                    resultado = self._parsear_input_output(valor_input)
                    var_uuaa_table1=resultado[1].split("_")
                    var_uuaa_table=var_uuaa_table1[1]
                    capa, tabla=resultado
                    #print(var_uuaa_table)
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
            #print(var_uuaa)
            return tipo_conf,capa,tabla,var_uuaa

   
    def _leer_catalogo_reglas(self,ruta_archivo):#Revisar este codigo
        with open(ruta_archivo, 'r') as archivo:
            catalogo = json.load(archivo)
            #print(ruta_archivo)#,catalogo)
        return catalogo

    def _is_critical(self, ruta_conf, regla):
        try:
            return regla.get("config").get("isCritical")
        except exceptions.ConfigMissingException:
            print(f"\nNo se indica isCritical en {ruta_conf} > id = {regla.get("config").get("id")}\n")
            return True
    
    def _obtener_campo_aplicado(self, ruta_conf, regla):#Modificado
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

    
    def _componentes(self, local_repo_path,ruta_file,file_name_componentes):#Modificado
        
        datos_repos = []
        archivo_repo=os.listdir(local_repo_path)
        
        for repositorios in archivo_repo:
            nombre_repos=os.path.basename(repositorios)#Obtengo el nombre de los repositorios
            var_rutacompleta=os.path.join(local_repo_path,repositorios)
            ruta_modi2=var_rutacompleta.replace("\\", "/")
            rutas_confs=self._obtener_archivos_conf(ruta_modi2)
            archivos_jenk=os.listdir(ruta_modi2)#Obtengo los archivos jenkins

            #Se crea inicialmente un diccionario por las diferentes longitudes de los repos
            dato_repo={'nombre repo':nombre_repos} #En este diccionario obtengo nom y ver
            
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
        
        #Luego de obtener los datos en las listas de cada campo
        #Creamos un dataframe a partir de la recopilación de los datos de los repositorios
        
        df = pd.concat([pd.DataFrame(datos) for datos in datos_repos], ignore_index=True)
        
        #ruta file y name file
        var_ruta=os.path.join(ruta_file,file_name_componentes)
        var_rutacompleta=var_ruta.replace("\\", "/")
        df.to_csv(var_rutacompleta, index=False)

        return df
        
    #Función toma como input el df generado por el pivot y nombre del archivo csv    
    def _FormatoDF(self,df_pivottable,ruta_file,file_name):

        df_reglas = df_pivottable
                    
        #considerar las reglas 3.1, 3.2,4.2,6.9
        #renombramos las columnas
        df_reglas.rename(columns={'Repositorio':'REPOSITORIO','Componente':'CONFIGURACION','Capa':'CAPA','Tabla':'ID OBJETO',
                                          'Id Funcional':'ID FUNCIONAL','Principio de Calidad':'PRINCIPIO DE CALIDAD',
                                          'Tipo de regla':'TIPO DE REGLA','clase':'CLASE','campo aplicado':'ID CAMPO',
                                          'isCritical':'REGLA BLOQUEANTE','acceptanceMin':'PORCENTAJE MINIMO DE ACEPTACION',
                                          'withRefusals':'TIPO DE MUESTRA DE RESULTADOS KOS','treatEmptyValuesAsNulls 3.1':
                                          '3.1 - TRATAMIENTO DE VACIOS COMO NULOS','formato 3.2':'3.2 - FORMATO',
                                          'columns 4.2': '4.2 - CAMPOS CLAVE DEL OBJETO','dataValuesSubset 6.9': 
                                          '6.9 - CONDICION DE FILTRADO','lowerBound 6.9':'6.9 - VARIACION MINIMA RELATIVA DE REGISTROS',
                                          'upperBound 6.9':'6.9 - VARIACION MAXIMA RELATIVA DE REGISTROS',},inplace=True) 
        
        #Listar las columnas a mostrar
        column_order=['REPOSITORIO','CONFIGURACION','CAPA','ID OBJETO','ID FUNCIONAL','PRINCIPIO DE CALIDAD',
                      'TIPO DE REGLA','CLASE','ID CAMPO','REGLA BLOQUEANTE','PORCENTAJE MINIMO DE ACEPTACION',
                      'TIPO DE MUESTRA DE RESULTADOS KOS','3.1 - TRATAMIENTO DE VACIOS COMO NULOS','3.2 - FORMATO',
                      '4.2 - CAMPOS CLAVE DEL OBJETO','6.9 - CONDICION DE FILTRADO','6.9 - VARIACION MINIMA RELATIVA DE REGISTROS',
                      '6.9 - VARIACION MAXIMA RELATIVA DE REGISTROS']
        #Ordenar las columnas
        df_reglas_ultimo = df_reglas.reindex(columns=column_order)
        
        #ruta file y name file
        var_ruta=os.path.join(ruta_file,file_name)
        var_rutacompleta=var_ruta.replace("\\", "/")
        df_reglas_ultimo.to_csv(var_rutacompleta, index=False)

     

    def _reglas(self, local_repo_path,ruta_file,file_name):
       datos_reglas1 = []
       datos_reglas2 = []
       archivo_repo=os.listdir(local_repo_path)
        
       for repositorios in archivo_repo:
            nombre_repos=os.path.basename(repositorios)#Obtengo el nombre de los repositorios
            var_rutacompleta=os.path.join(local_repo_path,repositorios)
            ruta_modi2=var_rutacompleta.replace("\\", "/")
            rutas_confs=self._obtener_archivos_conf(ruta_modi2)
            archivos_jenk=os.listdir(ruta_modi2)#Obtengo los archivos jenkins

            #Se crea inicialmente un diccionario por las diferentes longitudes de los repos
            dato_repo={'nombre repo':nombre_repos} #En este diccionario obtengo nom y ver

            for conf in rutas_confs:
                vconf_ruta_mod=conf.replace("\\", "/").strip()
                confs=os.path.basename(vconf_ruta_mod)
                catalogo_reglas = self._leer_catalogo_reglas("./tipos_de_reglas.json")
                #print(catalogo_reglas)
                #confs.append(os.path.basename(vconf_ruta_mod))
                
                tipo_conf = self._obtener_tipo_conf(vconf_ruta_mod)#kirby,hammurabi,histable
                #print(tipo_conf)
                if (tipo_conf=="hammurabi"):
                    config = ConfigFactory.parse_file(vconf_ruta_mod, resolve=False)
                    #table=
                    tipo_input_2, capa_input, tabla_input,uuaa_input = self._obtener_valor_input(vconf_ruta_mod)
                    #print(tipo_input_2, capa_input, tabla_input,uuaa_input)
                    tipo_input = config.get(tipo_conf).get("rules")
                    reglas = config.get(tipo_conf).get("rules")
                    for regla in reglas:
                        clase = regla.get("class")
                        
                        if clase.endswith("TemporalRule"):
                            temporals="Temporal"
                            clase = regla.get("config").get("parentClass")
                        else:
                            temporals=""
                        
                        #Formato de campos    
                        var_format=regla.get("config")                        
                        # Verificar si la clave "format" está presente en la configuración
                        if "format" in var_format:
                            # Si está presente, obtener su valor
                            valor_formato = var_format.get("format")
                        else:
                            # Si no está presente, mostrar nulo
                            valor_formato = ""
                                                                    
                        clases=clase
                        tipos=catalogo_reglas[clase].replace(',','.')
                        pcalidad=tipos[0]
                        ids=regla.get("config").get("id")
                        columnas=self._obtener_campo_aplicado(conf, regla)
                        criticals=self._is_critical(conf, regla)
                         
                        #Primary Key 
                        # Verificar si la clave "format" está presente en la configuración
                        if "keyColumns" in var_format:
                            # Si está presente, obtener su valor
                            valor_pk = var_format.get("keyColumns")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_pk = ""
                        
                        #acceptanceMin
                        # Verificar si la clave "format" está presente en la configuración
                        if "acceptanceMin" in var_format:
                            # Si está presente, obtener su valor
                            valor_accmin = var_format.get("acceptanceMin")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_accmin = ""
                        
                        #withRefusals
                        # Verificar si la clave "format" está presente en la configuración
                        if "withRefusals" in var_format:
                            # Si está presente, obtener su valor
                            valor_withre = var_format.get("withRefusals")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_withre = ""

                        #columns, identifica la regla 4.2
                        if "columns" in var_format:
                            # Si está presente, obtener su valor
                            valor_rule_42 = var_format.get("columns")
                        
                        else:
                            # Si no está presente, mostrar nulo
                            valor_rule_42 = ""

                        #treatEmptyValuesAsNulls regla 3.1
                        # Verificar si la clave "format" está presente en la configuración
                        if "treatEmptyValuesAsNulls" in var_format:
                            # Si está , obtener su valor
                            var_treatEmptyValuesAsNulls = var_format.get("treatEmptyValuesAsNulls")
                            
                        else:
                            # Si no está , mostrar el valor de default
                            var_treatEmptyValuesAsNulls = ""

                        #values
                        # Verificar si la clave "values" está presente en la configuración
                        if "values" in var_format:
                            # Si está presente, obtener su valor
                            valor_values = var_format.get("values")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_values = ""
                        
                        #subset
                        # Verificar si la clave "format" está presente en la configuración
                        if "subset" in var_format:
                            # Si está presente, obtener su valor
                            valor_subset = var_format.get("subset")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_subset = ""
                        
                        #dataValuesSubset
                        # Verificar si la clave "format" está presente en la configuración
                        if "dataValuesSubset" in var_format:
                            # Si está presente, obtener su valor
                            valor_dataValuesSubset = var_format.get("dataValuesSubset")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_dataValuesSubset = ""
                        
                        #condition
                        # Verificar si la clave "format" está presente en la configuración
                        if "condition" in var_format:
                            # Si está presente, obtener su valor
                            valor_condition = var_format.get("condition")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_condition = ""
                        
                        #rule 6.9 - lowerBound
                        if "lowerBound" in var_format:
                            # Si está presente, obtener su valor
                            valor_lowerBound = var_format.get("lowerBound")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_lowerBound = ""
                        
                        #rule 6.9 - upperBound
                        if "upperBound" in var_format:
                            # Si está presente, obtener su valor
                            valor_upperBound = var_format.get("upperBound")
                            
                        else:
                            # Si no está presente, mostrar nulo
                            valor_upperBound = ""
                         
                                                   
                        
                        dato1={
                            'Repositorio':nombre_repos,
                            'Componente':confs,
                            'Capa':capa_input,
                            'Tabla':tabla_input,
                            'Id Funcional':ids,
                            'Principio de Calidad':pcalidad,
                            'Tipo de regla':tipos,
                            'clase':clases,
                            'campo aplicado':columnas,
                            'isCritical':criticals,
                            'acceptanceMin':valor_accmin,
                            'withRefusals':valor_withre,
                            'treatEmptyValuesAsNulls':var_treatEmptyValuesAsNulls,
                            'formato':valor_formato,
                            'columns':valor_rule_42,
                            'lowerBound':valor_lowerBound,
                            'upperBound':valor_upperBound,
                            'dataValuesSubset':valor_dataValuesSubset
                            #'key column':valor_pk,                            
                            #'values':valor_values,                            
                            #'isTemporal':temporals,
                            #'subset':valor_subset,                            
                            #'condition':valor_condition                                  
                        }

                                              
                        #primeros campos. No se va pivotear
                        datos_reglas1.append(dato1)

                         #Creamos un dataframe a partir de la recopilación de los datos de los repositorios
       if datos_reglas1:
                df1 = pd.DataFrame(datos_reglas1)
                #DF casteado
                df1['acceptanceMin'] = df1['acceptanceMin'].astype(str)
                df1['campo aplicado']=df1['campo aplicado'].astype(str)
                
                for index, value in df1['treatEmptyValuesAsNulls'].items():
                    if pd.isna(value) or value == '':
                        df1.at[index, 'treatEmptyValuesAsNulls'] = "True"

                #Realizamos pivot table
                pivoted_df = df1.pivot_table(index=['Repositorio', 'Componente', 'Capa', 'Tabla', 'Id Funcional', 'Principio de Calidad', 'Tipo de regla','clase','campo aplicado','isCritical','acceptanceMin','withRefusals'],
                                                columns='Tipo de regla',
                                                #values=['key column','values','treatEmptyValuesAsNulls','formato','columns','isTemporal', 'subset','dataValuesSubset','condition','lowerBound','upperBound'],
                                                values=['treatEmptyValuesAsNulls','formato','columns','lowerBound','upperBound','dataValuesSubset'],
                                                aggfunc='first')
                
                
                # Reorganizar las columnas 
                pivoted_df.columns = [' '.join(col).strip() for col in pivoted_df.columns.values]
                pivoted_df.reset_index(inplace=True)

                #Realizamos merge de las columnas fijas con las columnas desagregadas
                merged_df = pd.merge(df1[['Repositorio', 'Componente', 'Capa', 'Tabla', 'Id Funcional', 'Principio de Calidad', 'Tipo de regla','clase','campo aplicado','isCritical','acceptanceMin','withRefusals']],
                     pivoted_df,
                     on=['Repositorio', 'Componente', 'Capa', 'Tabla', 'Id Funcional', 'Principio de Calidad', 'Tipo de regla','clase','campo aplicado','isCritical','acceptanceMin','withRefusals'],
                     how='outer', indicator=True)
                
                #Función _FormatoDF, toma como input el df pivoteado, 
                #luego tomar las columnas que se necesita y renombrar
                
                self._FormatoDF(merged_df,ruta_file,file_name)
                 
                return merged_df
        
      
    

       

       
        
        

                    
                
                
                    
                        
                       
        
        

                    

        
      
                    


            
        
       
    
            