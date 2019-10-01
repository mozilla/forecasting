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

---

For more information, contact jmccrosky@mozilla.com
