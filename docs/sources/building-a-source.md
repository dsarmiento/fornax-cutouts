---
title: Building a Source
description: Step-by-step tutorial for implementing and registering a mission source
---

# Building a Source

This guide walks through implementing a mission source from scratch and registering it with the Fornax Cutouts registry.

---

## What Is a Source?

A source is a Python class that answers one question: *given a list of sky positions, which FITS files contain data for those positions?*

The framework calls your source's `get_filenames()` method to build the list of files to cut. Everything else — job queuing, worker dispatch, cutout generation, result storage — is handled by Fornax Cutouts.

---

## Step 1: Create the Source File

Create a new `.py` file anywhere under your `CUTOUTS__SOURCE_PATH` directory:

```
sources/
└── my_mission.py
```

The file name does not matter. The registry discovers all `*.py` files under the configured path recursively.

---

## Step 2: Implement the Source Class

```python
from fornax_cutouts.sources import cutout_registry, AbstractMissionSource, MissionMetadata
from fornax_cutouts.models.base import Positions, TargetPosition
from fornax_cutouts.models.cutouts import FilenameLookupResponse, FilenameWithMetadata


@cutout_registry.register_source("my_mission")
class MySurveySource(AbstractMissionSource):
    metadata = MissionMetadata(
        name="my_mission",
        pixel_size=0.5,           # arcseconds per pixel
        max_cutout_size=2000,     # maximum cutout size in pixels
        filter=["g", "r", "i"],   # valid filter names
        survey=["wide"],          # valid survey names
    )

    def get_filenames(
        self,
        position: TargetPosition | Positions,
        filter: str | list[str] = ["g", "r", "i"],
        survey: str | list[str] = ["wide"],
        *args,
        include_metadata: bool = False,
        **kwargs,
    ) -> list[FilenameLookupResponse]:
        filter = self._cast_list_parameter(filter)
        survey = self._cast_list_parameter(survey)

        results = []
        for ra, dec in position:
            for f in filter:
                filename = self._lookup_file(ra, dec, f)
                if filename is None:
                    continue

                results.append(
                    FilenameLookupResponse(
                        mission="my_mission",
                        target=(ra, dec),
                        filenames=[FilenameWithMetadata(filename=filename)],
                    )
                )

        return results

    def _lookup_file(self, ra: float, dec: float, filter: str) -> str | None:
        # Replace with your actual file lookup logic
        return f"s3://my-bucket/data/{filter}/{ra:.4f}_{dec:.4f}.fits"
```

### Key points

- The `@cutout_registry.register_source("my_mission")` decorator registers the class under the key `"my_mission"`. This key is used in API requests as the mission name.
- `metadata` is a class-level attribute, not an instance attribute.
- `get_filenames()` receives the positions as a `TargetPosition` (single position) or `Positions` (list of `(ra, dec)` tuples). Always handle both cases.
- Use `self._cast_list_parameter()` to normalize `filter` and `survey` from either a string or list to a list.
- Return an empty list if no files are found — never raise an exception for missing data.

---

## Step 3: Define MissionMetadata

`MissionMetadata` describes the capabilities of your source. The framework uses it to validate requests before dispatching jobs.

```python
MissionMetadata(
    name="my_mission",       # must match the register_source() key
    pixel_size=0.5,          # arcseconds per pixel (used for coordinate math)
    max_cutout_size=2000,    # requests larger than this are rejected with 422
    filter=["g", "r", "i"],  # valid values for the filter parameter
    survey=["wide"],         # valid values for the survey parameter
)
```

`MissionMetadata` allows extra fields beyond the standard ones. Use this for mission-specific metadata you want to expose via the `/missions` endpoint:

```python
MissionMetadata(
    name="my_mission",
    pixel_size=0.5,
    max_cutout_size=2000,
    filter=["g", "r", "i"],
    survey=["wide"],
    # Mission-specific extras
    wavelength_range="400-900nm",
    coverage_area="full sky",
)
```

---

## Step 4: Implement File Lookup

The core of your source is the file lookup logic. The approach depends on how your survey's metadata is stored.

### Option A: DuckDB + Parquet (recommended for large catalogs)

This is the pattern used by the PS1 source. Parquet files are queried directly with DuckDB, which supports both local files and S3:

```python
import duckdb
from fornax_cutouts.sources import cutout_registry, AbstractMissionSource, MissionMetadata
from fornax_cutouts.models.base import Positions, TargetPosition
from fornax_cutouts.models.cutouts import FilenameLookupResponse, FilenameWithMetadata


@cutout_registry.register_source("my_mission")
class MySurveySource(AbstractMissionSource):
    metadata = MissionMetadata(
        name="my_mission",
        pixel_size=0.5,
        max_cutout_size=2000,
        filter=["g", "r", "i"],
        survey=["wide"],
    )

    _metadata_path = "s3://my-bucket/metadata/files.parquet"

    def __init__(self):
        self.conn = duckdb.connect()
        # Configure S3 access if needed
        self.conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.conn.execute("SET s3_region='us-east-1';")

    def get_filenames(
        self,
        position: TargetPosition | Positions,
        filter: str | list[str] = ["g", "r", "i"],
        survey: str | list[str] = ["wide"],
        *args,
        include_metadata: bool = False,
        **kwargs,
    ) -> list[FilenameLookupResponse]:
        filter = self._cast_list_parameter(filter)

        # Convert positions to RA/Dec arrays
        positions = list(position) if not isinstance(position, list) else position
        ra_min = min(p[0] for p in positions) - 0.1
        ra_max = max(p[0] for p in positions) + 0.1
        dec_min = min(p[1] for p in positions) - 0.1
        dec_max = max(p[1] for p in positions) + 0.1

        filter_list = ", ".join(f"'{f}'" for f in filter)
        query = f"""
            SELECT path, ra, dec, filter
            FROM read_parquet('{self._metadata_path}')
            WHERE ra BETWEEN {ra_min} AND {ra_max}
              AND dec BETWEEN {dec_min} AND {dec_max}
              AND filter IN ({filter_list})
        """

        results = []
        for row in self.conn.execute(query).fetchall():
            path, ra, dec, filt = row
            results.append(
                FilenameLookupResponse(
                    mission="my_mission",
                    target=(ra, dec),
                    filenames=[FilenameWithMetadata(
                        filename=path,
                        metadata={"filter": filt} if include_metadata else None,
                    )],
                )
            )

        return results
```

### Option B: Simple path construction

For surveys with predictable file naming conventions:

```python
def get_filenames(
    self,
    position: TargetPosition | Positions,
    filter: str | list[str] = ["g"],
    *args,
    include_metadata: bool = False,
    **kwargs,
) -> list[FilenameLookupResponse]:
    filter = self._cast_list_parameter(filter)

    positions = [position] if isinstance(position, TargetPosition) else list(position)
    results = []

    for ra, dec in positions:
        for f in filter:
            # Construct a deterministic path from coordinates
            tile = self._sky_to_tile(ra, dec)
            path = f"s3://my-bucket/{f}/{tile}.fits"

            results.append(
                FilenameLookupResponse(
                    mission="my_mission",
                    target=(ra, dec),
                    filenames=[FilenameWithMetadata(filename=path)],
                )
            )

    return results
```

---

## Step 5: Custom Validation (Optional)

Override `validate_request()` to add mission-specific validation beyond the built-in size and filter checks:

```python
def validate_request(self, size: int, **extras) -> bool:
    # Run the standard checks first
    is_valid = super().validate_request(size=size, **extras)

    # Add custom checks
    epoch = extras.get("epoch")
    if epoch is not None:
        is_valid &= epoch in ["2010", "2015", "2020"]

    return is_valid
```

---

## Step 6: Verify Registration

Start the API and check that your source appears in the missions list:

```bash
fornax-cutouts api
curl http://localhost:8000/api/v0/missions
```

Expected response includes your mission:

```json
{
  "my_mission": {
    "name": "my_mission",
    "pixel_size": 0.5,
    "max_cutout_size": 2000,
    "filter": ["g", "r", "i"],
    "survey": ["wide"]
  }
}
```

---

## Step 7: Submit a Test Job

```bash
curl -X POST http://localhost:8000/api/v0/cutouts/async \
  -F "position[]=83.8221,-5.3911" \
  -F "size=500" \
  -F "my_mission.filter=r" \
  -F "my_mission.survey=wide"
```

Follow the redirect to poll the job status:

```bash
curl http://localhost:8000/api/v0/cutouts/async/{job_id}/phase
```

---

## Checklist

- [ ] Source file is under `CUTOUTS__SOURCE_PATH`
- [ ] Class is decorated with `@cutout_registry.register_source("mission_key")`
- [ ] `metadata.name` matches the `register_source()` key
- [ ] `get_filenames()` handles both `TargetPosition` and `Positions` input
- [ ] `get_filenames()` returns an empty list (not an exception) when no files are found
- [ ] `filter` and `survey` are normalized with `_cast_list_parameter()`
- [ ] Source appears in `GET /api/v0/missions` after startup
