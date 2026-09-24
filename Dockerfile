FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=120 --retries 5 \
    -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt
COPY app.py sources.yaml ./
ENV LYSOURCE_DATA=/app/data
VOLUME /app/data
EXPOSE 8790
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8790"]
