FROM python:3.12.10-bookworm
LABEL org.opencontainers.image.authors="NC_SIT_ZHANGLE"

COPY . .

RUN pip install -r requirements.txt -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple

RUN cp .env.example .env

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "6783", "--timeout-graceful-shutdown", "1", "--reload", "--reload-exclude", "'*/tests/*'"]
