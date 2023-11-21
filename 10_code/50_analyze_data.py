"""
In this code the propensity score matching is done to find counterfactuals for the treatment group.
"""

import pandas as pd
import numpy as np


# Importing data about chronic diseases
chronic_disease_df = pd.read_csv(
    "https://data.cdc.gov/api/views/g4ie-h725/rows.csv?accessType=DOWNLOAD"
)
