# On part de l'image officielle TimescaleDB HA qui est basée sur Debian (PostgreSQL 15 + PostGIS + TimescaleDB)
# Cette image inclut nativement beaucoup d'extensions ou facilite leur installation.
FROM timescale/timescaledb-ha:pg15-latest

USER root

# Installation de pgvector (RAG)
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-15 \
    git \
    && git clone --branch v0.6.0 https://github.com/pgvector/pgvector.git \
    && cd pgvector \
    && make \
    && make install \
    && cd .. && rm -rf pgvector \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER postgres
