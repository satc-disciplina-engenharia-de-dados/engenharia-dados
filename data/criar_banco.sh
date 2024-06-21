#!/bin/bash

psql -U $POSTGRES_USER -c "CREATE DATABASE seguro;"
psql -d seguro -U $POSTGRES_USER -f docker-entrypoint-initdb.d/ddl_criar_banco.sql
psql -d seguro -U $POSTGRES_USER -f docker-entrypoint-initdb.d/dump_seguro.sql
echo "Banco de dados criado com sucesso!"
