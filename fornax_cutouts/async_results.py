import os
from dataclasses import dataclass, field
from io import BytesIO

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.io import votable
from astropy.table import Table as AstroTable
from fsspec import AbstractFileSystem, filesystem

from fornax_cutouts.constants import CUTOUT_STORAGE_PREFIX, DEPLOYMENT_TYPE

# This will need to move to another module
AWS_S3_REGION = os.getenv("AWS_S3_REGION")
if DEPLOYMENT_TYPE == "aws":
    import os

    import boto3

    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    # Set the environment variables so DuckDB can use them
    os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
    if credentials.token:
        os.environ["AWS_SESSION_TOKEN"] = credentials.token


@dataclass
class AsyncCutoutResults:
    job_id: str
    results_dir: str = field(init=False)

    def __post_init__(self):
        self.results_dir = f"{CUTOUT_STORAGE_PREFIX}/cutouts/{self.job_id}/results/"
        self.__duckdb_conn = duckdb.connect()
        self.__fs: AbstractFileSystem = filesystem("local")

        if self.results_dir.startswith("s3://"):
            self.__duckdb_conn.install_extension("httpfs")
            self.__duckdb_conn.load_extension("httpfs")
            self.__duckdb_conn.query(f"SET s3_region='{AWS_S3_REGION}';")
            self.__fs = filesystem("s3")

        if not self.__fs.isdir(self.results_dir):
            self.__fs.mkdir(self.results_dir)

    def __write_results_file(self, results: list, batch_num: int):
        results_fname = f"{self.results_dir}/results_{batch_num}.parquet"
        if self.__fs.exists(results_fname):
            raise FileExistsError("Results file already exists, not overwriting")
        self.__fs.touch(results_fname)
        results_t = pa.Table.from_pylist(results)
        with self.__fs.open(results_fname, "wb") as f:
            pq.write_table(results_t, f)

    def __update_size_file(self, new_count: int):
        size_path = f"{self.results_dir}/size"
        size_count = 0
        if self.__fs.exists(size_path):
            with self.__fs.open(size_path, "r") as f:
                existing = f.read()
            try:
                size_count = int(existing.strip())
            except Exception as e:
                print(f"Error updating size file: {e}")
                size_count = 0

        size_count += new_count
        with self.__fs.open(size_path, "w") as f:
            f.write(str(size_count))

    def add_results(self, results: list, batch_num: int):
        self.__update_size_file(len(results))
        self.__write_results_file(results, batch_num)

    def __get_results(self, page: int, size: int) -> pd.DataFrame:
        results_db = self.__duckdb_conn.read_parquet(f"{self.results_dir}/results_*.parquet")
        curr_results = results_db.limit(size, offset=page*size)
        return curr_results.to_df()

    def to_votable(self, page: int = 0, size: int = 100) -> str:
        df = self.__get_results(page, size)
        astro_t = AstroTable.from_pandas(df)
        vo_t = votable.from_table(astro_t)
        vo_io = BytesIO()
        vo_t.to_xml(vo_io)
        return vo_io.getvalue().decode()

    def to_json(self, page: int = 0, size: int = 100) -> str:
        df = self.__get_results(page, size)
        return df.to_json(orient='records')

    def to_csv(self, page: int = 0, size: int = 100) -> str:
        df = self.__get_results(page, size)
        return df.to_csv(index=False)
