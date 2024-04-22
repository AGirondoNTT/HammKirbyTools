# kirbyhamm_tools

Conjunto de herramientas orientadas al trabajo con ficheros de configuración de kirby o hammurabi.

> **Nota:** Por ahora solo se ha desarrollado la clase LectorReglasRepo.

## Instalar

1. Descargar el repositorio: `git clone https://github.com/jonorontt/kirbyhamm_tools.git`
2. Entrar en la carpeta y crear un entorno virtual: `python -m venv env`
3. Activar el entorno virtual: `env/Scripts/activate`
4. Instalar todas las dependencias: `pip install -r requirements.txt`

## LectorReglasRepo

### Métodos

- `componentes("ruta/al/repositorio")`: Devuelve un DataFrame con los datos a nivel de componente.
    - **nombre repo:** El nombre del repositorio.
    - **versión:** La versión obtenida del Jenkinsfile.
    - **nombre componente**: El nombre del componente.
    - **tipo**: Hammurabi/Kirby/Otro
    - **capa:** Master/Raw
    - **tabla:** Tabla input
    - **output:** (capa, tabla output)

- `reglas("ruta/al/repositorio")`: Devuelve un DataFrame con los datos a nivel de regla.
    - **REPOSITORIO** El nombre del componente al que pertenece la regla
	- **CONFIGURACION** Archivo de configuración de las reglas
	- **CAPA** Capa donde se encuentra el ID Objeto. Por ejemplo: Staging/Raw/Master
	- **ID OBJETO** Nombre de la tabla donde se aplican las reglas
	- **ID FUNCIONAL** ID de la regla
	- **PRINCIPIO DE CALIDAD** Regla principal. Regla 2,3,4,5,6
	- **TIPO DE REGLA** Tipo de regla. 2.1,3.1...,6.9
	- **CLASE** Nombre de la clase 
	- **ID CAMPO** Campos donde se aplican las reglas 
	- **REGLA BLOQUEANTE** Hace referencia al parámetro isCritical, indica si es crítica
	- **PORCENTAJE MINIMO DE ACEPTACION** Hace referencia al parámetro acceptanceMin
	- **TIPO DE MUESTRA DE RESULTADOS KOS** Hace referencia al parámetro withRefusals
	- **3.1 - TRATAMIENTO DE VACIOS COMO NULOS** Hace referencia al parámetro treatEmptyValuesAsNulls
	- **3.2 - FORMATO** Hace referencia al parámetro format 
	- **4.2 - CAMPOS CLAVE DEL OBJETO** Hace referencia al parámetro columns
	- **6.9 - CONDICION DE FILTRADO** Hace referencia al parámetro dataValuesSubset
	- **6.9 - VARIACION MINIMA RELATIVA DE REGISTROS** Hace referencia al parámetro lowerBound
	- **6.9 - VARIACION MAXIMA RELATIVA DE REGISTROS** Hace referencia al parámetro upperBound


### Done

- Se analiza todos los repositorios de la carpeta Test.

## Futuras Clases por Desarrollar

...