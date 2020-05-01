# anomdtct - Anomaly detection models

This directory contains the models for anomaly detection developed by Jesse McCrosky using the fbprophet framework. The pipeline provides the data for the [Opening data to understand social distancing](https://blog.mozilla.org/data/2020/03/30/opening-data-to-understand-social-distancing/) blog post.

## Setup

You will need a python environment with `fbprophet` and a few other
dependencies installed. We provide a Docker image that can be pulled from GCR
and run interactively like:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json
docker run -it \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
  -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/key.json:ro \
  --entrypoint python \
  gcr.io/moz-fx-data-forecasting/anomdtct
```

Or you can make code updates and build the image locally:

```bash
docker build . --tag anomdtct
docker run -it \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
  -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/key.json:ro \
  --entrypoint python \
  anomdtct
```

If you want to produce a local environment outside docker, you can create an
appropriate Conda environment via `conda env create -f environment.yml`.
To create an environment outside Conda, see the
[`fbprophet` installation instructions](https://facebook.github.io/prophet/docs/installation.html)
and then use `pip install -r requirements.txt` to install remaining dependencies.

---

For more information, contact jmccrosky@mozilla.com
