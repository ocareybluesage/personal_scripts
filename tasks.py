from invoke import task
from os import listdir
from pandas import DataFrame
import pandas as pd

COST_DIR = "./costs"

@task
def analyze_account_data(_):

    for file_name in [f for f in listdir(COST_DIR)]:
        df: DataFrame = pd.read_csv(f"{COST_DIR}/{file_name}")
        df = df.drop(index=[0])
        df = df.drop(['Tax($)', 'Total costs($)'], axis=1)
        df = df.rename(columns={'Service': 'Month'})
        df = df.melt(id_vars=["Month"], var_name="Service", value_name="Cost")

        df_top3 = df.sort_values(["Month", "Cost"], ascending=[True, False]).groupby("Month").head(3)

        print(f"----------------- {file_name.split('.csv')[0]} -----------------")
        print(df_top3)
        print("\n")




    