FROM python:3.12.10-bookworm

ARG VCS_REF=unknown
ARG IMAGE_VERSION=dev
ARG SOURCE_URL=https://github.com/zl875136491/storagent
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

LABEL org.opencontainers.image.authors="NC_SIT_ZHANGLE" \
      org.opencontainers.image.title="Storagent Backend" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.component="backend"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBUG=false \
    RELOAD=false \
    INIT_SERVICE=true

WORKDIR /app

RUN groupadd --gid 10001 storagent \
    && useradd --uid 10001 --gid storagent --create-home --shell /usr/sbin/nologin storagent \
    && install -d -o storagent -g storagent /app/logs

COPY requirements.txt ./requirements.txt
RUN python -m pip install \
      --no-cache-dir \
      --disable-pip-version-check \
      --index-url "${PIP_INDEX_URL}" \
      -r requirements.txt

COPY --chown=storagent:storagent main.py __init__.py storagent.sh ./
COPY --chown=storagent:storagent src ./src
COPY runtimes/mc /usr/local/bin/mc
RUN chmod 0755 /usr/local/bin/mc /app/storagent.sh

USER storagent:storagent

EXPOSE 9000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.getenv('SERVER_PORT', '9000') + '/ready', timeout=4)" || exit 1

CMD ["./storagent.sh", "run"]
