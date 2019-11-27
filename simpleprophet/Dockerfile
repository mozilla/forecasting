# Installing fbprophet on a bare python image takes some effort and time,
# particularly since pandas and some other libraries must be compiled from
# source. We use Anaconda instead and take advantage of pre-compiled packages.
# See https://facebook.github.io/prophet/docs/installation.html#python
FROM continuumio/miniconda3:4.7.12
RUN conda install -c conda-forge fbprophet==0.5

WORKDIR /app
RUN conda install pip
COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app
CMD ./entrypoint
