#!/bin/bash
podman build -f Dockerfile -t dagster-dev ../ && podman run --env-file ../.env -p 3000:3000 dagster-dev