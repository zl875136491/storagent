FROM python:3.12.10-bookworm
LABEL org.opencontainers.image.authors="NC_SIT_ZHANGLE"

WORKDIR /app
COPY . .
RUN rm .env
RUN mv runtimes/mc /usr/local/bin/mc
RUN chmod +x /usr/local/bin/mc

RUN pip install -r requirements.txt -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple

RUN cp .env.example .env

CMD ["./storagent.sh", "run"]