FROM continuumio/miniconda3:4.7.12

WORKDIR /app
COPY environment.yml /app
COPY requirements.txt /app
RUN conda env create -f environment.yml
ENV PATH /opt/conda/envs/anomdtct/bin:$PATH
COPY . /app
ENTRYPOINT ["/app/entrypoint"]
