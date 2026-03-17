#!/bin/bash

partial_url=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)

config_file="/tmp/conf/dm_svc.conf"

mkdir -p /tmp/conf/ && chmod -R 777 /tmp/conf/ && touch /tmp/conf/dm_svc.conf && chmod -R 777 /tmp/conf/dm_svc.conf


# shellcheck disable=SC2124
binary_path="$@"
run() {
  host=${RDS_HOST}

  echo "The 'host' value is: $host"
  dm_line="DM=($host)"
  cat <<EOF > "$config_file"
TIME_ZON=(480)
LANGUAGE=(cn)
$dm_line
[DM]
LOGIN_MODE=(1)
EOF
  echo "Host value has been written to $config_file"
  exec "/app/dist/data-model-management/data-model-management" "migrations"
}


run
