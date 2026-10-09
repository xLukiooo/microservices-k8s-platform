FROM python:3.11-slim 

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -u 10001 -d /app -s /bin/false appuser

COPY --chown=10001:10001 src/ ./src/

USER appuser

EXPOSE 9102

CMD ["python", "-m", "src.worker"]