FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

WORKDIR /dataflow/template

COPY streaming_pipeline.py ./
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=streaming_pipeline.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt
