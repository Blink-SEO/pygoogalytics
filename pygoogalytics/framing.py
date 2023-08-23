import logging
import re
import csv
import os
import numpy as np
import pandas as pd
import datetime

from google.analytics.data_v1beta.types.analytics_data_api import RunReportResponse

from typing import List, Optional, Union, Pattern

from .utils import general_utils
from .utils.ga4_parser import parse_ga4_response, parse_ga3_response



