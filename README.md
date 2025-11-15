# AnalisisDatos

Abrir una terminal en la raiz del archivo:

docker-compose up -d   

en esa misma terminal, cuando ya se creen los contenedores:

docker exec -it spark-client /opt/spark/bin/pyspark

Esto generará una terminal de spark en la que se podrán probar los scripts que están en la carpeta notebooks 