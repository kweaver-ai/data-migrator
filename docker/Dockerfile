FROM acr.aishu.cn/ict/data-model-management:5.1.0

LABEL stage=builder
WORKDIR /app

COPY config.yaml .
COPY docker/entrypoint.sh .
COPY repos ./repos
COPY requirements.txt .

RUN set -ex; \
  pip3 config set global.index-url http://mirrors.aliyun.com/pypi/simple/; \
  pip3 config set install.trusted-host mirrors.aliyun.com; \
  pip3 install -r requirements.txt;

# 移动模块化服务升级脚本
RUN chmod +x /app/entrypoint.sh
ENV DM_SVC_PATH="/tmp/conf/"

CMD /app/entrypoint.sh /app/dist/data-model-management/data-model-management