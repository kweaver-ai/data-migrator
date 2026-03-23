FROM ubuntu:24.04

WORKDIR /app

COPY server ./server

RUN set -ex; \
  apt-get update; \
  apt-get install -y git python3-pip; \
  pip3 config set global.index-url http://mirrors.aliyun.com/pypi/simple/; \
  pip3 config set install.trusted-host mirrors.aliyun.com; \
  pip3 install -r server/requirements.txt --break-system-packages; \
  apt-get clean; \
  rm -rf /var/lib/apt/lists/;

RUN set -ex; \
    echo "/usr/local/lib/python3.12/dist-packages/ksycopg2" > /etc/ld.so.conf.d/ksycopg2.conf; \
    ldconfig;

CMD ["python3", "server/data-migrator.py"]
