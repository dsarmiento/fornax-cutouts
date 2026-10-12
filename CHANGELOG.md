## 0.1.0 (2026-10-07)

Initial public development snapshot of the Fornax Cutouts framework (commits through [#37](https://github.com/dsarmiento/fornax-cutouts/pull/37)).

### Features

- Initialize the Fornax Cutouts project ([#1](https://github.com/dsarmiento/fornax-cutouts/pull/1))
- Support IVOA UWS parameters ([#2](https://github.com/dsarmiento/fornax-cutouts/pull/2))
- Job results, Click CLI, and `MissionSource` updates ([#3](https://github.com/dsarmiento/fornax-cutouts/pull/3))
- Batch jobs and mission metadata on `get_filenames` ([#4](https://github.com/dsarmiento/fornax-cutouts/pull/4))
- Metadata query performance improvements ([#7](https://github.com/dsarmiento/fornax-cutouts/pull/7))
- Memory management for worker cutout execution ([#8](https://github.com/dsarmiento/fornax-cutouts/pull/8))
- Load-testing harness updates ([#10](https://github.com/dsarmiento/fornax-cutouts/pull/10))
- Refactor cutout execution for synchronous endpoints
- Color preview endpoint and Celery task ([#11](https://github.com/dsarmiento/fornax-cutouts/pull/11))
- Standard pagination models ([#12](https://github.com/dsarmiento/fornax-cutouts/pull/12))
- Benchmark preparation ([#13](https://github.com/dsarmiento/fornax-cutouts/pull/13))
- Structured JSON logging for the API service ([#6](https://github.com/dsarmiento/fornax-cutouts/pull/6))
- Cutout queue priority; synchronous cutouts routed through Celery workers ([#16](https://github.com/dsarmiento/fornax-cutouts/pull/16))
- Sandbox deployment, scaling metric (`total_pending_tasks`), and logging cleanup ([#17](https://github.com/dsarmiento/fornax-cutouts/pull/17))
- Redis helper revamp ([#18](https://github.com/dsarmiento/fornax-cutouts/pull/18))
- Rolling-window cutout rate limits ([#21](https://github.com/dsarmiento/fornax-cutouts/pull/21))
- Multi-mission filename endpoints aligned with async job form parameters ([#22](https://github.com/dsarmiento/fornax-cutouts/pull/22))
- File-count endpoint ([#23](https://github.com/dsarmiento/fornax-cutouts/pull/23))
- ASDF cutout support ([#24](https://github.com/dsarmiento/fornax-cutouts/pull/24))
- Mission logging via filename source lookup ([#28](https://github.com/dsarmiento/fornax-cutouts/pull/28))
- Async task queueing and non-blocking cutout result rendering ([#29](https://github.com/dsarmiento/fornax-cutouts/pull/29))
- Async TTL for job metadata ([#30](https://github.com/dsarmiento/fornax-cutouts/pull/30))
- S3 signed URL configuration ([#32](https://github.com/dsarmiento/fornax-cutouts/pull/32))

### Bug Fixes

- File lookup total count ([#15](https://github.com/dsarmiento/fornax-cutouts/pull/15))
- Color preview header handling and filename passing ([#26](https://github.com/dsarmiento/fornax-cutouts/pull/26))
- Client IP resolution in request logging middleware ([#27](https://github.com/dsarmiento/fornax-cutouts/pull/27))
- API test error handling; FastAPI dependency pin to avoid extra Cloud CLI ([#31](https://github.com/dsarmiento/fornax-cutouts/pull/31))
- Do not read filter parameters from cutout files ([#35](https://github.com/dsarmiento/fornax-cutouts/pull/35))
- GitHub Pages documentation deployment hotfix
- Source registration uses mission metadata name

### Documentation

- Initial MyST documentation ([#14](https://github.com/dsarmiento/fornax-cutouts/pull/14))
- README and architecture diagram updates
- Documentation refresh ([#33](https://github.com/dsarmiento/fornax-cutouts/pull/33), [#34](https://github.com/dsarmiento/fornax-cutouts/pull/34))
- Contributing guide ([#37](https://github.com/dsarmiento/fornax-cutouts/pull/37))

### Build and CI

- AWS deployment and infrastructure updates ([#5](https://github.com/dsarmiento/fornax-cutouts/pull/5))
- Refresh AWS deployment tokens ([#9](https://github.com/dsarmiento/fornax-cutouts/pull/9))
- Expanded async API tests with `fakeredis` ([#25](https://github.com/dsarmiento/fornax-cutouts/pull/25))
- Semantic-release workflow, PyPI publishing, and conventional-commit PR title checks ([#37](https://github.com/dsarmiento/fornax-cutouts/pull/37))
