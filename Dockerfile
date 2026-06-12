FROM quay.io/astronomer/astro-runtime:11.3.0

# System dependencies
USER root
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER astro
