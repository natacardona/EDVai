# Google Skills Boost - Examen Final

## Realizar el siguiente LAB, al finalizar pegar un print screen donde se ve su perfil y el progreso final verificado

![Lab:](https://github.com/natacardona/EDVai/blob/main/FinalTest/NumberThree/Files/LabDataPrep.png)

## Título: Creating a Data Transformation Pipeline with Cloud Dataprep
## Schedule: 1 hour 15 minutes
## Cost: 5 Credits
## Link:
https://www.cloudskillsboost.google/focuses/4415?catalog_rank=%7B%22rank%22%3A1%2C%22num_filters%22%3A0%2C%22has_search%22%3Atrue%7D&parent=catalog&search_id=32278924


# Contestar las siguientes preguntas:

## 1. ¿Para que se utiliza data prep?
<p>Google Cloud Dataprep es una herramienta de preparación de datos en la nube que permite a los usuarios limpiar, transformar y enriquecer los datos antes de su análisis y procesamiento. Se utiliza principalmente para simplificar y automatizar el proceso de preparación de datos, haciéndolo más rápido y menos propenso a errores.</p>

## 2. ¿Qué cosas se pueden realizar con DataPrep?
<p> Con DataPrep, se pueden realizar diversas tareas como:

- Limpieza de datos (eliminar duplicados, corregir errores).
- Transformación de datos (cambiar formatos, crear nuevas columnas a partir de datos existentes).
- Enriquecimiento de datos (combinar datos de múltiples fuentes).
- Visualización de datos durante el proceso de preparación para identificar patrones y anomalías.</p>

## 3. ¿Por qué otra/s herramientas lo podrías reemplazar? Por qué?
<p> DataPrep podría ser reemplazado por herramientas como Talend, Informatica, y Microsoft Power Query, dependiendo de las necesidades específicas del proyecto. Estas herramientas también proporcionan funcionalidades robustas para la integración, transformación y limpieza de datos. La elección entre ellas puede depender de factores como la integración con otros sistemas, la escala de datos a manejar, y preferencias de la infraestructura existente. </p>

## 4. ¿Cuáles son los casos de uso comunes de Data Prep de GCP?
<p> Preparación de datos para análisis de big data.
Integración y limpieza de datos de diversas fuentes para business intelligence.
Automatización de flujos de trabajo de datos para mejorar la eficiencia de procesos analíticos.
Preparación de datos para entrenamiento de modelos de machine learning.</p>

## 5. ¿Cómo se cargan los datos en Data Prep de GCP?
<p> Los datos se pueden cargar en DataPrep desde varias fuentes como Google Cloud Storage, BigQuery, y otras fuentes compatibles con la conexión a Google Cloud. Se pueden cargar archivos directamente o conectar DataPrep a bases de datos existentes.</p>

## 6. ¿Qué tipos de datos se pueden preparar en Data Prep de GCP?
<p> DataPrep admite una variedad de tipos de datos, incluyendo datos estructurados como CSV, JSON, y bases de datos SQL, así como datos semi-estructurados.</p>

## 7. ¿Qué pasos se pueden seguir para limpiar y transformar datos en Data Prep de GCP?
<p> Los pasos típicos incluyen:
Importar datos desde una fuente.
Usar la interfaz de usuario para aplicar transformaciones como filtros, divisiones y conversiones.
Visualizar los datos para revisar los cambios.
Aplicar más transformaciones según sea necesario.
Exportar los datos transformados a un destino como BigQuery o Cloud Storage.
</p>

## 8. ¿Cómo se pueden automatizar tareas de preparación de datos en Data Prep de GCP?
<p> Se pueden automatizar flujos de trabajo de preparación de datos creando recetas (conjuntos de transformaciones) y utilizando desencadenadores programados o basados en eventos para ejecutar estas recetas automáticamente.</p>

## 9. ¿Qué tipos de visualizaciones se pueden crear en Data Prep de GCP?
<p>DataPrep permite crear visualizaciones como histogramas, gráficos de barras, gráficos de líneas y dispersión durante el proceso de preparación de datos para ayudar a entender mejor los datos y los efectos de las transformaciones aplicadas. </p>

## 10. ¿Cómo se puede garantizar la calidad de los datos en Data Prep de GCP?
<p>Para garantizar la calidad de los datos, DataPrep ofrece funcionalidades como la validación de datos, donde se pueden definir reglas y condiciones que los datos deben cumplir. Además, permite la inspección visual de datos para identificar y corregir problemas antes de finalizar el proceso de preparación. </p>


# Arquitectura:
<p> El gerente de Analitca te pide realizar una arquitectura hecha en GCP que contemple el uso de
esta herramienta ya que le parece muy fácil de usar y una interfaz visual que ayuda a sus
desarrolladores ya que no necesitan conocer ningún lenguaje de desarrollo.
Esta arquitectura debería contemplar las siguiente etapas:
Ingesta: datos parquet almacenados en un bucket de S3 y datos de una aplicación que guarda
sus datos en Cloud SQL.</p>

- Procesamiento: filtrar, limpiar y procesar datos provenientes de estas fuentes
- Almacenar: almacenar los datos procesados en BigQuery
- BI: herramientas para visualizar la información almacenada en el Data Warehouse
- ML: Herramienta para construir un modelo de regresión lineal con la información almacenada
en el Data Warehouse
