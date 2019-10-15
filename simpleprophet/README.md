# simpleprophet KPI forecasting models and tools

This directory contains the simpleprophet forecast models and accompanying tools.  The simpleprophet models prioritize simplicity and stability, adding complexity and "bendiness" only when it significantly improves model performance on a holdback period.

## Usage

The functions for running the forecasting pipeline are in ```pipeline.py```.  The ```update_table``` function can be run daily to add rows to the output table as necessary to incorporate newly available metrics data.  The ```replace_table``` function will clear the output table and regenerate it from scratch.

For model-building the code in ```modeling.py``` may be useful.  It includes a function to evaluate a model on a holdout set and provide some useful visualizations.

The ```validations.py``` file contains code that produces plots to evaluate model performance and validate the model behavior over time is reasonable.  It can also be used to compare multiple models.

## Modeling Strategy

The ```models.py``` file contains the production model specifications.  The models were developed by Jesse McCrosky using the fbprophet framework.  The guiding modeling philosophy was to be guided by simplicity and intuitive fit, while informing the modeling process using conventional types of quantitative evidence.

The evaluation of a forecast is fundamentally multi-dimensional.  As well as the competing objectives of stability, accuracy, and non-bias, each of these objectives can be evaluated on multiple time horizons.  This complexity makes a pure machine learning optimization approach extremely complex.

As an alternative, I chose to fit the models somewhat intuitively.  Parameter sets were explored iteratively and each iteration was evaluated visually to see if the model components (seasonality and trend) seemed to fit the observed actuals.  Once a reasonable parameter space was defined, the modeling process proceeded to evaluate holdout set metrics and other quantitative evaluations on a set of models.  Simpler models were preferred and complexity was only added when clearly justified by the quantitative evidence.

A few relevant model characteristics:

 - Due to the "smoothed" nature of MAU as a metric, yearly seasonality was adequate to capture all holiday effects except for Easter, which was included as a model component.
 - We select a start date for the training data based on the point where the metric appears to have reached a somewhat steady state in its development - the first weeks of most metrics are quite atypically and their use for training would not be helpful.
 - Similarly, some product metrics have "anomalies" - periods during which the metric value was highly atypical, usually due to a data problem.  These periods were excluded from training data.
 - The appropriate start dates and anomaly periods were determined through manual examination of metric plots.


---

For more information, contact jmccrosky@mozilla.com