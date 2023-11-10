# 📊 Assessing the Effects of Opioid Control Policies

Rafael Davila 

Jiwon Shin 

Bárbara Flores

### Introduction

In the last two decades, the United States has witnessed a significant increase in the use and abuse of prescription opioids, resulting in a growing addiction to these medications and an increase in deaths, both from overdoses of these drugs and those caused by non-prescription opioids such as heroin and fentanyl. This is because individuals addicted to prescription opioids may end up seeking other drugs due to the addiction they have developed. In response to this situation, certain policies have been implemented to control opioid prescriptions. The aim of this analysis is to understand whether these policies have achieved the desired effect.


### Research question
Given this scenario, we want to address the research questions:

- *What is the impact of opioid drug prescription regulations on the volume of opioids prescribed?*
- *What is the impact of opioid drug prescription regulations on drug overdose deaths?*


To address these causal inference questions, we will utilize two research methodologies: Pre-Post Comparison and Difference-in-Difference. In particular, this analysis will assess the effects of opioid prescription restriction policies in the states of Florida, Texas, and Washington. These states have recently implemented such policies, providing us with valuable insights into their impact.


**1. Pre-Post Comparison analysis**

In the 'Pre-Post Comparison' analysis, we will compare how things were in the states of Florida, Texas, and Washington just before the policies changed in the recent period to how they were after their implementation. In this analysis, we will be assuming that if the policies had not changed, these states in the post-change period would have looked similar to how they appeared in the pre-change period.


**2. Difference-in-Difference analysis**

We will conduct a more sophisticated analysis to address certain limitations of the pre-post comparison approach. We will employ the 'difference-in-difference' method. Instead of solely comparing Florida, Texas, and Washington before and after the policy changes, we will inquire whether there were more significant changes in overdose deaths in these states following the policy changes compared to other states that did not alter their opioid policies. In this analysis, we will include a linear regression to estimate the 'difference-in-difference' statistically. 


### Data

In particular, to carry out these analyses, we will utilize three databases:

**1. Opioid Prescriptions** 

This dataset is already included in our project inputs and comprises information on all prescription opioid drug shipments in the United States from 2006 to 2019.

**2. Vital Statistics Mortality Data:**

This dataset is also provided and corresponds to records from the US Vital Statistics, containing information about every death in the United States. It's important to note that a strategy must be devised for handling missing data, especially in cases where the number of individuals in a specific category (e.g., one county/year/cause of death category) is fewer than 10; in such instances, that data does not appear in the dataset.

**Population:** 

To account for the volume of drug overdose deaths and the number of opioids prescribed per capita, we will require the population data for each county by year (or a dataset that allows us to estimate these values). One of the tasks at hand is to locate a dataset that fulfills these requirements.


Here you can find the data of drug shipments for [Florida](https://dl.dropboxusercontent.com/scl/fi/dzsz8qffzwyz9l3tftvgr/arcos_all_washpost_FL.parquet?rlkey=es6vf6um49wdedjf5ggohuv5d&dl=0), Washington and Texas.
