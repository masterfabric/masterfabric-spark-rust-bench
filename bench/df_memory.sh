#!/usr/bin/env bash
# DataFusion memory diagnosis at one size: same container limits as the benchmark (6 CPU, 8 GiB), one variant
# per container. Output: results/raw/df_memory_<size>.jsonl (+ .exit per variant that failed).
set -uo pipefail
cd "$(dirname "$0")/.."
SIZE=${SIZE:-200m}
OUT=results/raw/df_memory_$SIZE.jsonl
mkdir -p results/raw; [ -n "${APPEND:-}" ] || : > "$OUT"
ONLY_VARIANTS=${ONLY_VARIANTS:-}
v() {  # name env...
  local name=$1; shift
  if [ -n "$ONLY_VARIANTS" ] && [[ " $ONLY_VARIANTS " != *" $name "* ]]; then return; fi
  local envs=(); for e in "$@"; do envs+=(-e "$e"); done
  docker run --rm --name mf-srb-dfmem --cpus 6 --memory 8g ${envs[@]+"${envs[@]}"} -v "$PWD":/w -w /w mf-srb/engines:2 \
    python bench/df_memory.py /w/data "$SIZE" "/w/$OUT" "$name"
  local rc=$?
  [ $rc -ne 0 ] && echo "{\"variant\": \"$name\", \"exit\": $rc}" >> "$OUT" && echo "!! $name exited $rc"
}
v baseline
v glibc_arena2 MALLOC_ARENA_MAX=2
v spill_pool_4g DF_MEMORY_POOL_GIB=4
v session_per_run SESSION_PER_RUN=1
v mimalloc_purge0 MIMALLOC_PURGE_DELAY=0
v mimalloc_purge0_pool4g MIMALLOC_PURGE_DELAY=0 DF_MEMORY_POOL_GIB=4
v mimalloc_segments2 MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2
v pause12 PAUSE_S=12
echo done
