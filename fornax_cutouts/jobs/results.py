from __future__ import annotations

import json
from dataclasses import dataclass
from io import BytesIO
from typing import Any

import boto3
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.io import votable
from astropy.io.votable.tree import Info, Link
from astropy.table import Table as AstroTable
from fsspec import AbstractFileSystem, filesystem

from fornax_cutouts.config import CONFIG
from fornax_cutouts.constants import AWS_S3_REGION
from fornax_cutouts.models.cutouts import CutoutResponse


@dataclass
class CutoutResults:
    job_id: str

    def __post_init__(self):
        self.results_dir = f"{CONFIG.storage.prefix}/cutouts/async/{self.job_id}/results"
        self.results_path_template = f"{self.results_dir}/results_{{}}.parquet"

        self.__duckdb_conn = duckdb.connect()
        self.__fs: AbstractFileSystem = filesystem("local")
        self.__is_s3 = False

        if self.results_dir.startswith("s3://"):
            self.__is_s3 = True
            self.__duckdb_conn.install_extension("httpfs")
            self.__duckdb_conn.load_extension("httpfs")

            session = boto3.Session()
            credentials = session.get_credentials().get_frozen_credentials()

            self.__duckdb_conn.query(f"SET s3_region='{AWS_S3_REGION}';")
            self.__duckdb_conn.query("SET s3_use_ssl=true;")
            self.__duckdb_conn.query("SET s3_url_style='path';")
            if credentials.access_key:
                self.__duckdb_conn.query(f"SET s3_access_key_id='{credentials.access_key}';")
            if credentials.secret_key:
                self.__duckdb_conn.query(f"SET s3_secret_access_key='{credentials.secret_key}';")
            if credentials.token:
                self.__duckdb_conn.query(f"SET s3_session_token='{credentials.token}';")

            self.__fs = filesystem("s3")

        if not self.__is_s3 and not self.__fs.isdir(self.results_dir):
                self.__fs.mkdir(self.results_dir)

    def __del__(self):
        self.__duckdb_conn.close()
        del self.__duckdb_conn
        del self.__fs

    def __write_results_file(self, results: list[CutoutResponse], batch_num: int):
        results_fname = self.results_path_template.format(batch_num)

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

    def add_results(self, results: list[CutoutResponse], batch_num: int):
        self.__write_results_file(results, batch_num)

    def __get_results(self, page: int, size: int) -> pd.DataFrame:
        try:
            results_db = self.__duckdb_conn.read_parquet(self.results_path_template.format("*"))
            curr_results = results_db.limit(size, offset=page * size)
            return curr_results.to_df()
        except duckdb.IOException as e:
            print(f"Error getting results: {e}")
            return pd.DataFrame()


    def __get_pagination_metadata(self, page: int, limit: int) -> dict[str, dict[str, Any]]:
        """
        Generate pagination links for the current page.

        Returns a dictionary with link IDs as keys and URLs as values.
        """
        try:
            results_db = self.__duckdb_conn.read_parquet(self.results_path_template.format("*"))
            total_items = results_db.count("*").fetchone()[0]
            total_pages = (total_items + limit - 1) // limit
        except duckdb.IOException:
            total_pages = 0

        base_url = f"/api/v0/cutouts/async/{self.job_id}/results/cutouts"
        metadata = {
            "metadata": {
                "page": page,
                "limit": limit,
                "totalItems": total_items,
                "totalPages": total_pages,
                "currentPage": page + 1
            },
            "links": {
                "self": f"{base_url}?page={page}&size={limit}",
                "first": f"{base_url}?page=0&size={limit}",
                "last": f"{base_url}?page={total_pages - 1}&size={limit}"
            },
        }

        if page > 0:
            metadata["links"]["prev"] = f"{base_url}?page={page - 1}&size={limit}"

        if page < total_pages - 1:
            metadata["links"]["next"] = f"{base_url}?page={page + 1}&size={limit}"

        return metadata

    def to_py(self, page: int = 0, limit: int = 100) -> dict:
        df = self.__get_results(page, limit)
        results = [CutoutResponse.model_validate(row.to_dict()) for _, row in df.iterrows()]

        resp = self.__get_pagination_metadata(page, limit)
        resp["results"] = results

        return resp

    def to_csv(self, page: int = 0, limit: int = 100) -> str:
        df = self.__get_results(page, limit)
        return df.to_csv(index=False)

    def to_votable(self, page: int = 0, limit: int = 100) -> str:
        df = self.__get_results(page, limit)
        astro_t = AstroTable.from_pandas(df)
        vo_t = votable.from_table(astro_t)

        pagination_metadata = self.__get_pagination_metadata(page, limit)

        for info_name, info_value in pagination_metadata["metadata"].items():
            info = Info(
                name=info_name,
                value=str(info_value),
            )
            vo_t.infos.append(info)

        for link_id, href in pagination_metadata["links"].items():
            link = Link(
                ID=link_id,
                content_role="query",
                content_type="application/xml",
                href=href,
                action="GET",
            )
            vo_t.resources[0].links.append(link)

        vo_io = BytesIO()
        vo_t.to_xml(vo_io)
        return vo_io.getvalue().decode()
