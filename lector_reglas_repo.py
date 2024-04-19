import os
import re
import json
from pyhocon import ConfigFactory, exceptions, HOCONConverter
import pandas as pd


class LectorReglasRepo:

    def _limpiar_str(self, cadena):
        return re.sub(r'[^a-zA-Z_]', '', cadena)
    
    def _parsear_input_output(self, valor_input):
        # Pasar de [ConfigValues: [ConfigSubstitution: MASTERSCHEMA],[ConfigQuotedString: .t_krea_re_dev_deg_inspections]] -> master
        capa = self._limpiar_str(valor_input.split(",")[0].split(":")[2]).lower()[:-6]
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

    def _obtener_archivos_conf(self,local_repo_path):
        confs = []

        for path, dirs, files in os.walk(local_repo_path):
            for filename in files:
                if filename.endswith(".conf"):
                    confs.append(os.path.join(path, filename))

        return confs

    def _obtener_tipo_conf(self, ruta_archivo):
        with open(ruta_archivo, 'r') as archivo:
            for linea in archivo:
                palabra_inicial = linea.split()[0]
                if palabra_inicial in ["hammurabi", "kirby"]:
                    return palabra_inicial
        
        return "otro"

    def _obtener_valor_input(self, ruta_archivo):
        config = ConfigFactory.parse_file(ruta_archivo, resolve=False)
        tipo_conf = self._obtener_tipo_conf(ruta_archivo)

        tipo_input = config.get(tipo_conf).get("input").get("type")

        capa = ""
        tabla = ""
        if (tipo_input == "parquet"):
            valor_input = config.get(tipo_conf).get("input").get("paths")[0]
            valor_input_split = valor_input.split("/")
            capa = valor_input_split[2]
            tabla = valor_input_split[-1]
        elif (tipo_input == "table"):
            valor_input = str(config.get(tipo_conf).get("input").get("tables")[0])
            capa, tabla = self._parsear_input_output(valor_input)
        else:
            valor_input = "Type no contemplado" # TODO: Anadir si hay más tipos de inputs

        return valor_input, capa, tabla

    def _obtener_valor_output(self, ruta_archivo):
        config = ConfigFactory.parse_file(ruta_archivo, resolve=False)

        tipo_output = config.get("kirby").get("output").get("type")

        capa = ""
        tabla = ""
        if (tipo_output == "parquet"):
            valor_output = config.get("kirby").get("output").get("paths")
        elif (tipo_output == "table"):
            valor_output = str(config.get("kirby").get("output").get("table"))
            capa, tabla = self._parsear_input_output(valor_output)
        else:
            valor_output = "Type no contemplado" # TODO: Anadir si hay más tipos de outputs

        return valor_output, capa, tabla

    def _leer_catalogo_reglas(self,ruta_archivo):
        with open(ruta_archivo, 'r') as archivo:
            catalogo = json.load(archivo)
        
        return catalogo

    def _is_critical(self, ruta_conf, regla):
        try:
            return regla.get("config").get("isCritical")
        except exceptions.ConfigMissingException:
            print(f"\nNo se indica isCritical en {ruta_conf} > id = {regla.get("config").get("id")}\n")
            return True
    
    def _obtener_campo_aplicado(self, ruta_conf, regla):
        try:
            return regla.get("config").get("column")
        except exceptions.ConfigMissingException:
            try:
                return regla.get("config").get("columns")
            except exceptions.ConfigMissingException:
                _, _, tabla = self._obtener_valor_input(ruta_conf)
                return tabla
    
    def componentes(self, local_repo_path):
        confs = []
        tipos = []
        inputs = []
        outputs = []
        capas = []
        tablas = []

        nombre_repo = os.path.basename(local_repo_path)
        version = self._obtener_version_jenkinsfile(os.path.join(local_repo_path, "Jenkinsfile"))
        rutas_confs = self._obtener_archivos_conf(local_repo_path)
        for ruta_conf in rutas_confs:
            confs.append(os.path.basename(ruta_conf))
            tipos.append(self._obtener_tipo_conf(ruta_conf))
            if (self._obtener_tipo_conf(ruta_conf) == "kirby"):
                valor_output, capa_output, tabla_output = self._obtener_valor_output(ruta_conf)
                outputs.append((capa_output, tabla_output))
            else:
                outputs.append("")
            valor_input, capa_input, tabla_input = self._obtener_valor_input(ruta_conf)
            inputs.append(valor_input)
            capas.append(capa_input)
            tablas.append(tabla_input)

        data = {
            'nombre repo': nombre_repo,
            'versión': version,
            'nombre componente': confs,
            'tipo': tipos,
            'capa': capas,
            'tabla': tablas,
            'output': outputs
        }

        return pd.DataFrame(data)


    def reglas(self, local_repo_path):
        confs = []
        ids = []
        tipos = []
        clases = []
        columnas = []
        criticals = []
        temporals = []
        rule_configs = []

        catalogo_reglas = self._leer_catalogo_reglas("./tipos_de_reglas.json")

        rutas_confs = self._obtener_archivos_conf(local_repo_path)
        for ruta_conf in rutas_confs:
            config = ConfigFactory.parse_file(ruta_conf, resolve=False)
            tipo_conf = self._obtener_tipo_conf(ruta_conf)
            
            if (tipo_conf == "kirby"):
                continue

            reglas = config.get(tipo_conf).get("rules")
            for regla in reglas:
                confs.append(os.path.basename(ruta_conf))
                
                clase = regla.get("class")
                if clase.endswith("TemporalRule"):
                    temporals.append("True")
                    clase = regla.get("config").get("parentClass")
                else:
                    temporals.append(None)
                
                clases.append(clase)
                tipos.append(catalogo_reglas[clase])
                ids.append(regla.get("config").get("id"))
                criticals.append(self._is_critical(ruta_conf, regla))
                columnas.append(self._obtener_campo_aplicado(ruta_conf, regla))
                rule_configs.append(HOCONConverter.to_json(regla.get("config"))) # ! Puede que de error cuando se quiera obtener el json de vuelta
                
        data = {
            'componente': confs,
            'id': ids, 
            'tipo': tipos,
            'clase': clases,
            'campo aplicado': columnas,
            'isCritical': criticals,
            'isTemporal': temporals,
            'config regla': rule_configs
        }

        return pd.DataFrame(data)


