FROM apache/airflow

# Copy the requirements.txt file into the container
COPY requirements.txt /requirements.txt

# Upgrade pip and install packages
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt