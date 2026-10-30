#!/usr/bin/env bash
# Pin and verify the official DuckDB library; avoid a large C++ source build.
set -euo pipefail
cd "$(dirname "$0")/.."
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=osx-arm64; sha=c35d60080b31ba5d11a3ce505a6831be784f003b4c575d32a4c2badb3b41cdf0 ;;
  Darwin-x86_64) platform=osx-amd64; sha=98fef9fcad856555ca0403fe4d9d6680acfd26c67e8c6f004d61a248d6bc0eff ;;
  Linux-x86_64) platform=linux-amd64; sha=993ad89e7dfca66cd6da994c2cad090c2fc160ddf9f550498a098ab4bb13cfb1 ;;
  Linux-aarch64) platform=linux-arm64; sha=c60eecf91143ca6bbac55c4f4d40dc22520488b0ac257c86e0fe9a2b451cf2b7 ;;
  *) echo 'Supported: macOS and glibc Linux, ARM64 or x86_64.' >&2; exit 1 ;;
esac
runtime="$PWD/.runtime/duckdb-2.0.0-$platform"
if [[ ! -f "$runtime/.verified" ]]; then
  mkdir -p "$runtime"
  archive="$runtime/library.tar.gz"
  curl --fail --location --retry 3 "https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-shared-libs-$platform.tar.gz" -o "$archive"
  actual=$(shasum -a 256 "$archive" | cut -d ' ' -f 1)
  [[ "$actual" = "$sha" ]] || { echo 'DuckDB checksum mismatch' >&2; exit 1; }
  tar -xzf "$archive" -C "$runtime"
  touch "$runtime/.verified"
fi
export DUCKDB_LIB_DIR="$runtime"
export LD_LIBRARY_PATH="$runtime${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export DYLD_LIBRARY_PATH="$runtime${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
exec cargo "$@"
