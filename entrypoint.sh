#!/bin/bash

# Espera Postgres estar pronto (usa pg_isready, instalado no Dockerfile)
until pg_isready -h postgres -p 5432 -U postgres -q; do
  echo "Aguardando Postgres..."
  sleep 1
done

# Init DB se não existir (ignore erros se já inited)
airflow db init || true

# Upgrade migrações
airflow db upgrade

# Cria connections default
airflow connections create-default-connections

# Cria user admin (ignore se já existe)
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true

# Roda scheduler em background (para parsear DAGs) e webserver
airflow scheduler &
airflow webserver