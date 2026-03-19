# Changelog

## [0.0.75] - 2026-03-18

- Moved API ingest step from `update_deejay_set_collection` to `process_new_files`. Now only newly processed CSVs are sent to deejay-marvel-api.
- Refactored `ingest_to_api.py` to expose `read_tracks_from_sheet` and `build_ingest_payload` as reusable functions.

## [0.0.74] - 2026-03-18

- Added `ingest_to_api.py` — new pipeline step that sends newly processed sets to deejay-marvel-api via POST `/v1/ingest` after collection update.
- Wired into `update_deejay_set_collection.py`. Pipeline skips API step gracefully if `KAIANO_API_BASE_URL` is not set.

## [0.0.71] - 2025-03-17

- Migrated from Poetry to uv, and from black/isort/flake8 to ruff.
- Updated README to fully describe processor purpose, inputs, outputs, and configuration.
