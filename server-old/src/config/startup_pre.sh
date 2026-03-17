#!/bin/sh

USERNAME=${USERNAME:-root}
PASSWORD=${PASSWORD:-eisoo.com}
PORT=${PORT:-3307}
HOST=${HOST:-10.4.110.85}
TYPE=${TYPE:-MYSQL}
ADMIN_KEY=${ADMIN_KEY:-''}
SOURCE_TYPE=${SOURCE_TYPE:-e}

/app/dist/data-model-management/data-model-management \
 --username "$USERNAME" \
 --password "$PASSWORD" \
 --port "$PORT" \
 --host "$HOST" \
 --type "$TYPE" \
 --admin_key "$ADMIN_KEY" \
 --env_mode tiduyun  \
 --script_directory_path /app/repos/   \
 --source_type "$SOURCE_TYPE" \
 migrations --stage pre