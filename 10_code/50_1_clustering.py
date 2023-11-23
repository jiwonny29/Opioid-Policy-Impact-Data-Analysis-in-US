import pandas as pd
import numpy as np

data = pd.read_csv("../20_intermediate_files/opioids_unemp_educ.csv")
# Computing morphine mg bought per capita
data["morphine_mg_pp"] = data["total_morphine_mg"] / data["tot_pop"]
# Computing percentage of population with more than 45 years old
data["pct_greater45"] = data["p_greater45"] / data["tot_pop"]
# We are going to compute three changes 2006-2007 (TX),2006-2010 (FL), 2006-2012 (WA)
variables_of_interest = [
    "State",
    "unemp_avg_rate",
    "morphine_mg_pp",
    "pct_greater45",
]
# 2006 is our oldest year, so every comparison should be considering that year
year = 2006
aux = data[data["year"] == year].copy()
aux = aux[variables_of_interest]
aux.rename(
    columns={
        "unemp_avg_rate": "unemp_rate_change_since_" + str(year),
        "morphine_mg_pp": "morphine_mg_pp_change_since_" + str(year),
        "pct_greater45": "pct_greater45_change_since_" + str(year),
    },
    inplace=True,
)
data = data.merge(aux, on="State", how="left")
# Computing the changes
data["unemp_rate_change_since_" + str(year)] = (
    data["unemp_avg_rate"] / data["unemp_rate_change_since_" + str(year)]
) - 1
data["morphine_mg_pp_change_since_" + str(year)] = (
    data["morphine_mg_pp"] / data["morphine_mg_pp_change_since_" + str(year)]
) - 1
data["pct_greater45_change_since_" + str(year)] = (
    data["pct_greater45"] / data["pct_greater45_change_since_" + str(year)]
) - 1

# Finding the three states for Florida
variables_of_interest = [
    "State",
    "year",
    "morphine_mg_pp",
    "unemp_rate_change_since_" + str(year),
    "morphine_mg_pp_change_since_" + str(year),
    "pct_greater45_change_since_" + str(year),
]

######### GETTING THE THREE STATES FOR EACH STATE ##########

# states of interest
states = {"TX": 2007, "FL": 2010, "WA": 2012}
final_variables = variables_of_interest + ["distance", "pairing_state"]
# Empty dataframe
results = pd.DataFrame(columns=final_variables)

for key in states.keys():
    state_of_interest = data[data["year"] == states[key]][variables_of_interest].copy()
    # standarizing the intake of morphine per capita
    state_of_interest["morphine_mg_pp"] = (
        state_of_interest["morphine_mg_pp"] - state_of_interest["morphine_mg_pp"].mean()
    ) / state_of_interest["morphine_mg_pp"].std()
    # computing fixed values to compute the distance
    unemp_st = state_of_interest[state_of_interest["State"] == key][
        "unemp_rate_change_since_" + str(year)
    ].values[0]
    morphine_st = state_of_interest[state_of_interest["State"] == key][
        "morphine_mg_pp_change_since_" + str(year)
    ].values[0]
    pct45_st = state_of_interest[state_of_interest["State"] == key][
        "pct_greater45_change_since_" + str(year)
    ].values[0]
    morphine_st_pp = state_of_interest[state_of_interest["State"] == key][
        "morphine_mg_pp"
    ].values[0]
    # Computing the euclidean distance
    state_of_interest["distance"] = np.sqrt(
        (state_of_interest["unemp_rate_change_since_" + str(year)] - unemp_st) ** 2
        + (state_of_interest["morphine_mg_pp_change_since_" + str(year)] - morphine_st)
        ** 2
        + (state_of_interest["pct_greater45_change_since_" + str(year)] - pct45_st) ** 2
        + (state_of_interest["morphine_mg_pp"] - morphine_st_pp) ** 2
    )

    # Sorting the values
    state_of_interest.sort_values(by="distance", inplace=True)

    # Selecting the top 3
    state_of_interest = state_of_interest.iloc[1:4, :]
    state_of_interest["pairing_state"] = key
    results = pd.concat([results, state_of_interest])

results.to_csv(
    "../20_intermediate_files/counterfactuals_by_euclidean_distance.csv", index=False
)
