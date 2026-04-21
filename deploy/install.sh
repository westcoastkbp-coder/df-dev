#!/usr/bin/env bash
set -eu

TARGET_DIR="/opt/digital_foreman"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

mkdir -p "$TARGET_DIR"
cp -R "$PROJECT_DIR/app" "$TARGET_DIR/"
cp -R "$PROJECT_DIR/runtime" "$TARGET_DIR/"
cp -R "$PROJECT_DIR/scripts" "$TARGET_DIR/"
chmod -R 755 "$TARGET_DIR/scripts"
