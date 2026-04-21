FROM python:3.12-slim

# System deps: pdftotext (poppler-utils) + Node.js (for wlop.js signing)
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Data mount points
RUN mkdir -p /data/invoices /data/output

ENV INVOICE_INPUT_DIR=/data/invoices
ENV INVOICE_OUTPUT_DIR=/data/output
ENV INVOICE_OUTPUT_PARENT=/data/output

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8787/api/tasks', timeout=5)" || exit 1

CMD ["python", "captcha_workbench.py", "--output-dir", "/data/output", "--host", "0.0.0.0", "--port", "8787"]
