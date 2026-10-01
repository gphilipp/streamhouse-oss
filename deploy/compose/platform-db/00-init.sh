#!/bin/bash
# Platform database: control-plane metadata, Context Engine serving tables,
# and the backing stores of Gravitino, the Iceberg REST catalog and Apicurio.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
  CREATE SCHEMA IF NOT EXISTS streamhouse_meta AUTHORIZATION streamhouse;
  CREATE SCHEMA IF NOT EXISTS serving AUTHORIZATION streamhouse;
  CREATE DATABASE gravitino OWNER streamhouse;
  CREATE DATABASE iceberg OWNER streamhouse;
  CREATE DATABASE apicurio OWNER streamhouse;
SQL

# Gravitino's relational entity store does not create its own tables on PostgreSQL.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname gravitino \
  -f /docker-entrypoint-initdb.d/gravitino-schema-1.3.0-postgresql.sql.in
