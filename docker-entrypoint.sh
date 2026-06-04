#!/bin/sh
set -eu

mkdir -p "${CHUNKGATE_DATA_DIR:-/data}"
chown -R chunkgate:chunkgate "${CHUNKGATE_DATA_DIR:-/data}"

exec su-exec chunkgate:chunkgate "$@"
