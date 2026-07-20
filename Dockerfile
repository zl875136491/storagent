FROM python:3.12.10-bookworm
LABEL org.opencontainers.image.authors="NC_SIT_ZHANGLE"

WORKDIR /app
COPY . .
RUN mv runtimes/mc /usr/local/bin/mc
RUN chmod +x /usr/local/bin/mc

RUN pip install -r requirements.txt -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple

ENV DEBUG=false
ENV RELOAD=false
ENV INIT_SERVICE=true

EXPOSE 9000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9000/ready')" || exit 1

CMD ["./storagent.sh", "run"]
