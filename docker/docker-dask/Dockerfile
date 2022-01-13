FROM daskdev/dask:2021.9.1-py3.8

COPY docker/python_requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -r requirements.txt