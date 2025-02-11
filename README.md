# Proyecto 1: Data Mining

## Descripción
Proyecto 1 de Data Mining.

Nombre: Ericson López (00326945)

## Tabla de Contenidos
- [Resumen](#resumen)
- [Instalación](#instalación)
- [Uso](#uso)

## Resumen
Proyecto de tratamiento inicial de data de instacart. Paso de RAW a CLEAN utilizando una limpieza a partir de un EDA y un modelamiento que permita analizar correctamente los datos para los fines de la empresa.

## Instalación
Instrucciones para instalar el proyecto.

```bash
# Clonar el repositorio
git clone https://github.com/usuario/proyecto1.git

# Navegar al directorio del proyecto
cd proyecto1

```
Instalar dependencias a partir de requirements.txt. Ya sea instalando directamente en python o con una máquina virtual:
```bash
pip install -r requirements.txt
```
```bash
# Crear una máquina virtual
python -m venv venv

# Activar la máquina virtual
# En Windows
venv\Scripts\activate
# En macOS/Linux
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```
Generar variables de entorno para poder configurar los accesos a MySQL y Snowflake. Se deberán crear las variables para MySQL: `DB_USER`, `DB_PASSWORD`, `DB_IP`, `DB_PORT` y para Snowflake: `SNOW_ACCOUNT`,`SNOW_USER`,`SNOW_PASSWORD`.

Importante asegurarse un correcto funcionamiento de Mage AI con Snowflake y el traspaso correcto de las variables de entorno en caso de usar contenedores.


## Uso

- Ejecutar el script `dataLoad.py` para cargar los datos de los CSV a MySQL.
- A través de `Mage AI` ejecutar el primer pipeline para pasar de BDD a RAW. Para este pipeline se ejecuta en orden el dataloader `dataloaderraw.py` y el data exporter `dataexporterraw.py`. También se puede exportar el pipeline a Mage a partir del archivo .zip `bdd_to_raw.zip`.
- Validar la consistencia de los datos de RAW ejecutando el notebook `validation.ipynb`
- Realizar un análisis exploratorio de datos (EDA) utilizando el script `eda.ipynb`.
- A través de `Mage AI` ejecutar el segundo pipeline para pasar de RAW a CLEAN. Para este pipeline se ejecuta en orden el dataloader `dataloaderclean.py`, el data transformer `raw_cleaning.py`, el data transformer `raw_modelling.py` y el data exporter `data_exporter_clean.py`.También se puede exportar el pipeline a Mage a partir del archivo .zip `raw_to_clean.zip`
- Validar la consistencia de los datos de CLEAN ejecutando el notebook `validation_clean.ipynb`
- Realizar el análisis de los datos tratados a través de `insights.ipynb`




