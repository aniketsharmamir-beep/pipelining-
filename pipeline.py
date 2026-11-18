"""
Simple data pipeline built with Luigi: Extract -> Clean -> Transform -> Load

Run:  python pipeline.py
"""

import os
import sqlite3

import luigi
import pandas as pd

SOURCE_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv"
OUT_DIR = "data/pipeline"
DB_PATH = f"{OUT_DIR}/pipeline.db"


def save_csv(df, target):
    """Write a DataFrame to the file that Luigi expects for this task."""
    os.makedirs(os.path.dirname(target.path), exist_ok=True)
    df.to_csv(target.path, index=False)


class ExtractData(luigi.Task):
    """Step 1: download the raw data from the source and save it."""

    def output(self):
        return luigi.LocalTarget(f"{OUT_DIR}/raw.csv")

    def run(self):
        df = pd.read_csv(SOURCE_URL)
        print(f"Extracted {len(df)} rows and {df.shape[1]} columns")
        save_csv(df, self.output())


class CleanData(luigi.Task):
    """Step 2: remove duplicates and empty rows, and fix inconsistent values."""

    def requires(self):
        return ExtractData()

    def output(self):
        return luigi.LocalTarget(f"{OUT_DIR}/clean.csv")

    def run(self):
        df = pd.read_csv(self.input().path)
        rows_before = len(df)

        df = df.drop_duplicates()
        df = df.dropna(subset=["body_mass_g"])           # rows with no measurements
        df["sex"] = df["sex"].str.title().fillna("Unknown")

        print(f"Cleaned data: {rows_before} -> {len(df)} rows")
        save_csv(df, self.output())


class TransformData(luigi.Task):
    """Step 3: create new columns from the existing ones."""

    def requires(self):
        return CleanData()

    def output(self):
        return luigi.LocalTarget(f"{OUT_DIR}/features.csv")

    def run(self):
        df = pd.read_csv(self.input().path)

        df["body_mass_kg"] = df["body_mass_g"] / 1000
        df["bill_ratio"] = (df["bill_length_mm"] / df["bill_depth_mm"]).round(2)
        df["size_group"] = pd.cut(
            df["body_mass_g"], bins=[0, 3500, 4750, 10000],
            labels=["Small", "Medium", "Large"],
        )

        print(f"Transformed data: {df.shape[1]} columns")
        save_csv(df, self.output())


class LoadData(luigi.Task):
    """Step 4: load the final table into a SQLite database."""

    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget(f"{OUT_DIR}/load_complete.txt")

    def run(self):
        df = pd.read_csv(self.input().path)

        conn = sqlite3.connect(DB_PATH)
        try:
            df.to_sql("penguins_clean", conn, if_exists="replace", index=False)
            rows = conn.execute("SELECT COUNT(*) FROM penguins_clean").fetchone()[0]
        finally:
            conn.close()

        print(f"Loaded {rows} rows into table penguins_clean")
        with open(self.output().path, "w") as f:
            f.write(f"Loaded {rows} rows into penguins_clean\n")


if __name__ == "__main__":
    luigi.build([LoadData()], local_scheduler=True)
