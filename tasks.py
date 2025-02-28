from invoke import task
from os import listdir
from pandas import DataFrame
import pandas as pd

COST_DIR = "./costs"


@task
def analyze_account_data(_):

    columns = [
        "Most Expensive AWS Service",
        "Largest Cost",
        "Second Most Expensive AWS Service",
        "Second Largest Cost",
        "Third Most Expensive AWS Service",
        "Third Largest Cost",
    ]

    output: DataFrame = pd.DataFrame(columns=["client", "Month"] + columns)

    for file_name in [f for f in listdir(COST_DIR)]:
        client = file_name.split(".csv")[0]
        df: DataFrame = pd.read_csv(f"{COST_DIR}/{file_name}", index_col=False)
        df = df.drop(index=[0])
        df = df.drop(["Tax($)", "Total costs($)"], axis=1)
        df = df.rename(columns={"Service": "Month"})
        df = df.melt(id_vars=["Month"], var_name="Service", value_name="Cost")

        df["Service"] = df["Service"].apply(lambda x: x.replace("($)", ""))
        df["Cost"] = df["Cost"].fillna(0)
        df["Cost"] = df["Cost"].apply(lambda x: round(x, 2))

        df = (
            df.sort_values(["Month", "Cost"], ascending=[True, False])
            .groupby("Month")
            .head(3)
        )

        df["Rank"] = df.groupby("Month").cumcount() + 1
        df = df.pivot(index="Month", columns="Rank", values=["Service", "Cost"])

        df.columns = [f"{col[0]}_{col[1]}" for col in df.columns]

        column_order = []
        [column_order.extend([f"Service_{i}", f"Cost_{i}"]) for i in range(1, 4)]
        df = df[column_order]

        df = df.rename(
            columns={
                "Service_1": "Most Expensive AWS Service",
                "Cost_1": "Largest Cost",
                "Service_2": "Second Most Expensive AWS Service",
                "Cost_2": "Second Largest Cost",
                "Service_3": "Third Most Expensive AWS Service",
                "Cost_3": "Third Largest Cost",
            }
        )

        df = df.reset_index()
        df.insert(0, "client", client)

        print(f"----------------- {client} -----------------")
        print(df)
        print("\n")

        output = pd.concat([output, df], ignore_index=True)

    output.to_csv("output.csv", index=False)
