# anomdtct - Anomaly detection models

This directory contains the models for anomaly detection developed by Jesse McCrosky using the fbprophet framework. The pipeline provides the data for the [Opening data to understand social distancing](https://blog.mozilla.org/data/2020/03/30/opening-data-to-understand-social-distancing/) blog post.

## Setup

You will need a python environment with `fbprophet` and a few other
dependencies installed.

### Model fitting

For making predictions, this pipeline expects that the forecasting models are cached in BigQuery.
To calculate models, we provide a Docker image that can be pulled from GCR
and run like:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json
docker run -it \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
  -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/key.json:ro \
  --entrypoint "/app/fit_models" \
  gcr.io/moz-fx-data-forecasting/anomdtct
```

### Forecasting

Once models have been cached in BigQuery, they can be used to make predictions.
We provide a Docker image that can be pulled from GCR and run like:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json
docker run -it \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
  -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/key.json:ro \
  --entrypoint "/app/entrypoint 2020-05-04" \
  gcr.io/moz-fx-data-forecasting/anomdtct
```

The `/app/entrypoint` script expects a date parameter.

You can make code updates and build the image locally:

```bash
docker build . --tag anomdtct
```

If you want to produce a local environment outside docker, you can create an
appropriate Conda environment via `conda env create -f environment.yml`.
To create an environment outside Conda, see the
[`fbprophet` installation instructions](https://facebook.github.io/prophet/docs/installation.html)
and then use `pip install -r requirements.txt` to install remaining dependencies.

---

For more information, contact jmccrosky@mozilla.com
