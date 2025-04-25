FROM bitnami/spark:3.5.3

# Cài pip + các thư viện cần thiết
RUN apt-get update && apt-get install -y python3-pip && \
    pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt
