#!/bin/bash

set -e

DIR="$(pwd)"
TMP_PATH="$(mktemp -d)/telegraf"
mkdir "${TMP_PATH}"

function cleanup() {
    rm -rf "${TMP_PATH}"
}
trap cleanup EXIT

git clone --depth 1 https://github.com/SumoLogic/telegraf.git "${TMP_PATH}"

cd "${TMP_PATH}" && go mod download
for OS in windows darwin linux; do
    echo "Building telegraf for ${OS}..."
    BINARY_PATH="${DIR}/telegraf_${OS}_amd64"
    GOOS=${OS} GOARCH=amd64 go build -o "${BINARY_PATH}" ./cmd/telegraf
done
