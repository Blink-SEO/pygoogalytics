# API utilities

## - Google API wrapper

`google_api_wrapper.py` defines the class `GapiWrapper` with 
methods to access data from the GA3 (UA), GSC and GA4 APIs via the 
python clients.

The GA4 client is an instance of the `BetaAnalyticsDataClient` class from ``google.analytics.data_v1beta`
provided by Google. The "Beta" part leads one to believe this is not a finished product 
so updates will have to be made.

The class `GapiWrapper` references the global variables `GA3_RESOURCE`, `GSC_RESOURCE` and `GA4_DATA_CLIENT` 
imported from the main module, but takes parameters
- sc_domain
- view_id
- ga4_property_id

so that each instance is particular to a client defined by these three parameters.

Using the methods of the global "client" objects, and passing 
the client-specific parameters, the GapiWrapper accesses data for
a particular client.

All data can be accessed using the method `get_df(result)` 
which returns a pandas dataframe, with the argument `result` 
determining which of GA3, GA4, GSC or GSC (url inspection) 
is accessed. Optional arguments of `get_df` are
- `dimensions` and `metrics` — for GA and GSC data.
- `start_date` and `end_date` — to make an API request for
  a given date range (for GA and GSC data).
- `url_list` can be passed when `result='URL'` 
  (i.e. requesting data from GSC url inspection) 
  to request an inspection for specified urls.

## - Google Pandas

`googlepandas.py` provides the classes `GADataFrame` and `GSCDataFrame`, 
both children of pandas `DataFrame` specific to storing 
GA and GSC data with additional metadata fields 
(e.g. `dimensions` and `metrics`) and methods for adding columns 
and filtering by particular metrics.






