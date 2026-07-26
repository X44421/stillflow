# StillFlow Rust backend

This service completes the local CSV flow used by the existing StillFlow UI:

1. Validate and persist an uploaded UTF-8 CSV.
2. Execute enabled pipeline nodes in order.
3. Return per-stage row and quality metrics.
4. Persist the cleaned CSV and expose it for preview or download.
5. Persist projects, pipeline snapshots, and project-scoped datasets.

## Run locally

Requirements: Rust 1.77 or newer.

```bash
npm run dev:backend
```

The service listens on `127.0.0.1:8787` by default. Start the frontend with
`npm run dev`; Vite proxies `/api` requests to the backend.

Configuration:

| Variable | Default | Purpose |
| --- | --- | --- |
| `STILLFLOW_BIND` | `127.0.0.1:8787` | HTTP bind address |
| `STILLFLOW_DATA_DIR` | `data` | Uploads, exports, `datasets.json`, and `projects.json` |
| `RUST_LOG` | `stillflow_backend=info,tower_http=info` | Log filters |

For a separately hosted frontend, set `VITE_API_URL` to the backend origin
before building the frontend.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | Liveness check |
| `GET` | `/api/projects` | List projects |
| `POST` | `/api/projects` | Create a project |
| `GET` | `/api/projects/:id` | Read one project |
| `PATCH` | `/api/projects/:id` | Rename or describe a project |
| `PUT` | `/api/projects/:id/workspace` | Save selected datasets and pipeline nodes |
| `DELETE` | `/api/projects/:id` | Delete a project and its datasets |
| `GET` | `/api/datasets?projectId=:id` | List datasets in a project |
| `POST` | `/api/datasets/import` | Upload multipart fields `projectId` and `file` |
| `PATCH` | `/api/datasets/:id` | Rename a dataset |
| `DELETE` | `/api/datasets/:id` | Delete a dataset and its stored file |
| `GET` | `/api/datasets/:id/preview?limit=100` | Preview up to 500 rows |
| `POST` | `/api/pipeline/run` | Run enabled nodes for one source dataset |
| `GET` | `/api/exports/:id/download` | Download the cleaned CSV |

Appending `?download=false` to the export URL opens the CSV inline.

Uploads are limited to 50 MB. The first version accepts UTF-8 CSV files with a
non-empty, unique header row. Cleaning is in memory, so this service is intended
for small local or single-user workloads. Existing `datasets.json` records are
assigned to the default project automatically when the service first starts.

## Test

```bash
npm run test:backend
```

The unit tests cover ordered transforms, preview statistics, and CSV
round-tripping.
