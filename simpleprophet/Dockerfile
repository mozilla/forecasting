# Installing fbprophet on a bare python image takes some effort and time,
# particularly since pandas and some other libraries must be compiled from
# source. We use Anaconda instead and take advantage of pre-compiled packages.
# See https://facebook.github.io/prophet/docs/installation.html#python
FROM continuumio/miniconda3:4.7.12

WORKDIR /app
COPY environment.yml /app
COPY requirements.txt /app
RUN conda env create -f environment.yml
ENV PATH /opt/conda/envs/simpleprophet/bin:$PATH
COPY . /app
ENTRYPOINT ["/app/entrypoint"]
