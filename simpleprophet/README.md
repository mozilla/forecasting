# simpleprophet KPI forecasting models and tools

This directory contains the simpleprophet forecast models and accompanying tools.  The simpleprophet models prioritize simplicity and stability, adding complexity and "bendiness" only when it significantly improves model performance on a holdback period.  More documentation to come.

## data.py

Tools for getting metric actual or forecast data from BigQuery.

## modeling.py

Tools for model development and evaluation.

## models.py

The current models and training data specification.

## output.py

Tools for writing forecasts to BigQuery.

## pipeline.py

Single functions for running the forecasting pipeline.

## utils.py

Various utility functions.

## validation.py

Tools for valdiation and evaluation of forecast models.

## Local development

To set up a local development environment, you'll need Python 3 and we
recommend using a virtual environment:

```
cd simpleprophet/
python3 -m venv venv
pip install -r requirements.txt
```

From that directory, you should be able to start up a python interpreter
and populate a new table with forecasts like:

```
from simpleprophet.pipeline import replace_table
from google.cloud import bigquery

bq_client = bigquery.Client()
replace_table(bq_client, table_id='<your_username>_simpleprophet')
```

As you develop, any dependency updates need to be captured in
`requirements.txt`. Upgrading `fbprophet` for example, could look like:

```
# Assumes you're in a venv with dependencies already installed.
pip install --upgrade fbprophet

# Save a snapshot of installed dependency versions back to the requirements file
pip freeze > requirements.txt
```

---

For more information, contact jmccrosky@mozilla.com
