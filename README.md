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
    - **componente:** El nombre del componente al que pertenece la regla
    - **id:** El ID de la regla.
    - **tipo:** Número de regla.
    - **clase:** Nombre de la clase de la regla.
    - **campo aplicado:** Campo sobre el que se aplica.
    - **isCritical:** Parámetro que indica si es crítica.
    - **isTemporal:** Parámetro que indica si es una TemporalRule.
    - **config regla:** JSON de todo el parámetro config de la regla, para posible uso futuro.

### ToDo

- Hacer que sea posible introducir una carpeta con varios repositorios dentro y que analize todos de una.

## Futuras Clases por Desarrollar

...