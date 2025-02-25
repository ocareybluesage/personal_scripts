from invoke import task
from os import listdir
from pandas import DataFrame
import pandas as pd

COST_DIR = "./costs"


@task
def analyze_account_data(_):

    for file_name in [f for f in listdir(COST_DIR)]:
        df: DataFrame = pd.read_csv(f"{COST_DIR}/{file_name}", index_col=False)
        df = df.drop(index=[0])
        df = df.drop(["Tax($)", "Total costs($)"], axis=1)
        df = df.rename(columns={"Service": "Month"})
        df = df.melt(id_vars=["Month"], var_name="Service", value_name="Cost")

        df_top3 = (
            df.sort_values(["Month", "Cost"], ascending=[True, False])
            .groupby("Month")
            .head(3)
        )

        print(f"----------------- {file_name.split('.csv')[0]} -----------------")

        df_top3["Top 3 Costs"] = df_top3["Service"] + ": " + df_top3["Cost"].astype(str)
        df = df_top3[["Month", "Top 3 Costs"]]

        print(df.groupby("Month")["Top 3 Costs"].agg(", ".join).reset_index())

        print("\n")
