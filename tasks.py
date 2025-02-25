from invoke import task
from os import listdir
from pandas import DataFrame
import pandas as pd

COST_DIR = "./costs"


@task
def analyze_account_data(_, head: str = "3"):
    head = int(head)
    top_columns = [str(i) for i in range(1, head + 1)]

    output: DataFrame = pd.DataFrame(columns=['client','Month'] + top_columns)

    for file_name in [f for f in listdir(COST_DIR)]:
        df: DataFrame = pd.read_csv(f"{COST_DIR}/{file_name}", index_col=False)
        df = df.drop(index=[0])
        df = df.drop(["Tax($)", "Total costs($)"], axis=1)
        df = df.rename(columns={"Service": "Month"})
        df = df.melt(id_vars=["Month"], var_name="Service", value_name="Cost")

        df_top = (
            df.sort_values(["Month", "Cost"], ascending=[True, False])
            .groupby("Month")
            .head(head)
        )

        df_top[f"Top {head} Costs"] = df_top["Service"] + ": " + df_top["Cost"].astype(str)
        df = df_top[["Month", f"Top {head} Costs"]]

        df_grouped = df.groupby("Month")[f"Top {head} Costs"].agg(list).reset_index()
        df_expanded = df_grouped[f"Top {head} Costs"].apply(lambda x: pd.Series(x[:head]))

        df_expanded.columns = top_columns

        df_final = pd.concat([df_grouped["Month"], df_expanded], axis=1)
        df_final.insert(0, 'client', file_name.split('.csv')[0])

        print(f"----------------- {file_name.split('.csv')[0]} -----------------")
        print(df_final)
        print("\n")

        output = pd.concat([output, df_final], ignore_index=True)

    output.to_csv("output.csv", index=False)
