FROM python:3.11

# WORKDIR /workdir

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/workdir

ENTRYPOINT ["python", "-m", "src.scripts.run_historical_load"]