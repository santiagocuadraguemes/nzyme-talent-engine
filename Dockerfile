# 1. IMAGEN BASE
# Usamos la imagen oficial de AWS para Python 3.11. 
# Esta imagen ya tiene el sistema operativo optimizado para Lambda.
FROM public.ecr.aws/lambda/python:3.11

# 2. INSTALACIÓN DE DEPENDENCIAS
# Primero copiamos solo el requirements.txt para aprovechar el caché de Docker
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Instalamos las librerías en la carpeta del sistema de Lambda
RUN pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir

# 3. COPIA DEL CÓDIGO FUENTE
# Copiamos tus carpetas de código a la variable ${LAMBDA_TASK_ROOT} (que suele ser /var/task)
COPY core/ ${LAMBDA_TASK_ROOT}/core/
COPY scripts/ ${LAMBDA_TASK_ROOT}/scripts/

# Copiamos el cerebro principal
COPY main_lambda.py ${LAMBDA_TASK_ROOT}

# NOTA DE SEGURIDAD:
# NO copiamos el archivo .env aquí. 
# Las claves secretas se configurarán directamente en el panel de AWS Lambda 
# como "Variables de Entorno" para mayor seguridad.

# 4. COMANDO DE ARRANQUE
# Le decimos a Lambda qué función ejecutar cuando se despierte.
# Formato: nombre_archivo.nombre_funcion
CMD [ "main_lambda.lambda_handler" ]