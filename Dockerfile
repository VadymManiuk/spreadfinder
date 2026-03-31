FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for layer caching
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# Copy application code
COPY . .

# Run as non-root
RUN useradd --create-home scanner
USER scanner

CMD ["python", "-m", "main"]
