import pandas as pd

# Household Median Income data (2006) cleaning
# changed my codes after getting feedbacks for using relative path
file_path1 = "00_source_data/Additional_datasets/median_household_inc06.xls"
income06 = pd.read_excel(file_path1, header=2)

selected_columns06 = [
    "State FIPS",
    "County FIPS",
    "Name",
    "Poverty Percent All Ages",
    "Median Household Income",
]
income06_upd = income06[selected_columns06]
income06_upd = income06_upd.iloc[1:3193]

income06_upd["State FIPS"] = income06_upd["State FIPS"].astype(str).str.zfill(2)
income06_upd["County FIPS"] = (
    income06_upd["County FIPS"].astype(int).astype(str).str.zfill(3)
)
income06_upd["FIPS"] = income06_upd["State FIPS"] + income06_upd["County FIPS"]

income06_upd.drop(["State FIPS", "County FIPS"], axis=1, inplace=True)
income06_upd["Year"] = 2006
income06_upd


# Household Median Income data (2011) cleaning
file_path2 = "00_source_data/Additional_datasets/median_household_inc11.xls"
income11 = pd.read_excel(file_path2, header=2)

selected_columns11 = [
    "State FIPS",
    "County FIPS",
    "Name",
    "Poverty Percent All Ages",
    "Median Household Income",
]
income11_upd = income11[selected_columns11]
income11_upd = income11_upd.iloc[1:3195]

income11_upd["State FIPS"] = income11_upd["State FIPS"].astype(str).str.zfill(2)
income11_upd["County FIPS"] = (
    income11_upd["County FIPS"].astype(int).astype(str).str.zfill(3)
)
income11_upd["FIPS"] = income11_upd["State FIPS"] + income11_upd["County FIPS"]

income11_upd.drop(["State FIPS", "County FIPS"], axis=1, inplace=True)
income11_upd["Year"] = 2011
income11_upd


# Household Median Income data (2016) cleaning
file_path3 = "00_source_data/Additional_datasets/median_household_inc16.xls"
income16 = pd.read_excel(file_path3, header=2)

selected_columns16 = [
    "State FIPS Code",
    "County FIPS Code",
    "Name",
    "Poverty Estimate, All Ages",
    "Median Household Income",
]
income16_upd = income16[selected_columns16]
income16_upd = income16_upd.iloc[1:3195]

income16_upd["State FIPS Code"] = (
    income16_upd["State FIPS Code"].astype(str).str.zfill(2)
)
income16_upd["County FIPS Code"] = (
    income16_upd["County FIPS Code"].astype(int).astype(str).str.zfill(3)
)
income16_upd["FIPS"] = (
    income16_upd["State FIPS Code"] + income16_upd["County FIPS Code"]
)

income16_upd.drop(["State FIPS Code", "County FIPS Code"], axis=1, inplace=True)
income16_upd = income16_upd.rename(
    columns={"Poverty Estimate, All Ages": "Poverty Percent All Ages"}
)
income16_upd["Year"] = 2016
income16_upd


# Household Median Income data (2019) cleaning
file_path4 = "00_source_data/Additional_datasets/median_household_inc19.xls"
income19 = pd.read_excel(file_path4, header=2)

selected_columns19 = [
    "State FIPS Code",
    "County FIPS Code",
    "Name",
    "Poverty Percent, All Ages",
    "Median Household Income",
]
income19_upd = income19[selected_columns16]
income19_upd = income19_upd.iloc[1:3195]

income19_upd["State FIPS Code"] = (
    income19_upd["State FIPS Code"].astype(str).str.zfill(2)
)
income19_upd["County FIPS Code"] = (
    income19_upd["County FIPS Code"].astype(int).astype(str).str.zfill(3)
)
income19_upd["FIPS"] = (
    income19_upd["State FIPS Code"] + income19_upd["County FIPS Code"]
)

income19_upd.drop(["State FIPS Code", "County FIPS Code"], axis=1, inplace=True)
income19_upd = income19_upd.rename(
    columns={"Poverty Estimate, All Ages": "Poverty Percent All Ages"}
)
income19_upd["Year"] = 2019
income19_upd

# Concatenate four Income datasets from 2006, 2011, 2016, 2019
combined_income = pd.concat([income06_upd, income11_upd, income16_upd, income19_upd])
new_order = [
    "FIPS",
    "Year",
    "Name",
    "Poverty Percent All Ages",
    "Median Household Income",
]
combined_income = combined_income[new_order]
combined_income
