# Usa la imagen oficial de Apache Airflow como base
FROM apache/airflow:2.9.0

# Otorga permisos al directorio /var/lib/apt/lists/partial
USER root
#RUN chmod 755 /var/lib/apt/lists/partial

# Instala wget
RUN apt-get update && apt-get install -y wget

# Configura cualquier otro requisito o configuración que necesites
