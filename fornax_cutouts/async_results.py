import json
from dataclasses import dataclass, field
from io import BytesIO

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.io import votable
from astropy.table import Table as AstroTable
from fsspec import AbstractFileSystem, filesystem

from fornax_cutouts.constants import AWS_S3_REGION, CUTOUT_STORAGE_PREFIX
from fornax_cutouts.models.cutouts import CutoutResponse
from fornax_cutouts.utils.aws import get_aws_credentials


@dataclass
class AsyncCutoutResults:
    job_id: str
    results_dir: str = field(init=False)

    def __post_init__(self):
        self.results_dir = f"{CUTOUT_STORAGE_PREFIX}/cutouts/{self.job_id}/results"
        self.results_path_template = f"{self.results_dir}/results_{{}}.parquet"

        self.__duckdb_conn = duckdb.connect()
        self.__fs: AbstractFileSystem = filesystem("local")

        if self.results_dir.startswith("s3://"):
            self.__duckdb_conn.install_extension("httpfs")
            self.__duckdb_conn.load_extension("httpfs")

            access_key, secret_key, token = get_aws_credentials()

            self.__duckdb_conn.query(f"SET s3_region='{AWS_S3_REGION}';")
            self.__duckdb_conn.query("SET s3_use_ssl=true;")
            self.__duckdb_conn.query("SET s3_url_style='path';")
            if access_key:
                self.__duckdb_conn.query(f"SET s3_access_key_id='{access_key}';")
            if secret_key:
                self.__duckdb_conn.query(f"SET s3_secret_access_key='{secret_key}';")
            if token:
                self.__duckdb_conn.query(f"SET s3_session_token='{token}';")

            self.__fs = filesystem("s3")

        if not self.__fs.isdir(self.results_dir):
            self.__fs.mkdir(self.results_dir)

    def __write_results_file(self, results: list[CutoutResponse], batch_num: int):
        results_fname = self.results_path_template.format(batch_num)
        if self.__fs.exists(results_fname):
            raise FileExistsError("Results file already exists, not overwriting")
        self.__fs.touch(results_fname)

        results_py = [
            {
                "mission": r.mission,
                "position": r.position,
                "size_px": r.size_px,
                "filter": r.filter,
                "mission_extras": json.dumps(r.mission_extras),
                "fits": r.fits,
                "preview": r.preview,
            }
            for r in results
        ]
        results_t = pa.Table.from_pylist(
            results_py,
            schema=pa.schema(
                [
                    pa.field("mission", pa.string()),
                    pa.field("position", pa.list_(pa.float64())),
                    pa.field("size_px", pa.list_(pa.int64())),
                    pa.field("filter", pa.string()),
                    pa.field("mission_extras", pa.string()),
                    pa.field("fits", pa.string()),
                    pa.field("preview", pa.string()),
                ]
            ),
        )

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

    def add_results(self, results: list[CutoutResponse], batch_num: int):
        self.__update_size_file(len(results))
        self.__write_results_file(results, batch_num)

    def __get_results(self, page: int, size: int) -> pd.DataFrame:
        try:
            results_db = self.__duckdb_conn.read_parquet(self.results_path_template.format("*"))
            curr_results = results_db.limit(size, offset=page * size)
            return curr_results.to_df()
        except duckdb.IOException as e:
            print(f"Error getting results: {e}")
            return pd.DataFrame()

    def to_py(self, page: int = 0, size: int = 100) -> list[CutoutResponse]:
        df = self.__get_results(page, size)
        return [CutoutResponse.model_validate(row.to_dict()) for _, row in df.iterrows()]

    def to_csv(self, page: int = 0, size: int = 100) -> str:
        df = self.__get_results(page, size)
        return df.to_csv(index=False)

    def to_votable(self, page: int = 0, size: int = 100) -> str:
        df = self.__get_results(page, size)
        astro_t = AstroTable.from_pandas(df)
        vo_t = votable.from_table(astro_t)
        vo_io = BytesIO()
        vo_t.to_xml(vo_io)
        return vo_io.getvalue().decode()
