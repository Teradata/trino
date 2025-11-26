#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run-shards.sh <num-shards> [--module-dir <path-to-module>] [--work-base <dir>] [--logs-dir <dir>]
#
# Example:
#   ./run-shards.sh 8
#   ./run-shards.sh 8 --module-dir plugin/trino-teradata --work-base /tmp/tt-shard --logs-dir ./trino-teradata/shard-results-logs
#
# Defaults assume repo layout like:
#   <repo-root>/mvnw
#   <repo-root>/plugin/trino-teradata
#
# The script will:
#  - detect repo root (git) or use current working directory heuristics
#  - copy module into per-shard work dirs using cp -a
#  - run each shard in its own JVM (parallel backgrounded processes)
#  - stream each shard output to console and to a per-shard log file

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <num-shards> [--module-dir <path-to-module>] [--work-base <dir>] [--logs-dir <dir>]"
  exit 1
fi

N="$1"; shift

# defaults
MODULE_REL_DEFAULT="plugin/trino-teradata"
WORK_BASE_DEFAULT="/tmp/tt-shard"
LOGS_DIR_DEFAULT=""   # if empty, default to <module-dir>/shard-results-logs
ROOT_MVNW_OVERRIDE=""

MODULE_REL="${MODULE_REL_DEFAULT}"
WORK_BASE="${WORK_BASE_DEFAULT}"
LOGS_DIR="${LOGS_DIR_DEFAULT}"

# parse optional args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --module-dir) MODULE_REL="$2"; shift 2;;
    --work-base) WORK_BASE="$2"; shift 2;;
    --logs-dir) LOGS_DIR="$2"; shift 2;;
    --mvnw) ROOT_MVNW_OVERRIDE="$2"; shift 2;;
    *) echo "Unknown option: $1"; exit 1;;
  esac
done

# find repo root (prefer git)
REPO_ROOT=""
if command -v git >/dev/null 2>&1; then
  if git rev-parse --show-toplevel >/dev/null 2>&1; then
    REPO_ROOT="$(git rev-parse --show-toplevel)"
  fi
fi

# fallback: try walking up from cwd to find mvnw or module dir
if [[ -z "$REPO_ROOT" ]]; then
  CWD="$(pwd)"
  SEARCH_LIMIT=6
  FOUND=""
  for i in $(seq 0 $SEARCH_LIMIT); do
    if [[ -f "$(pwd)/mvnw" && -d "$(pwd)/$MODULE_REL" ]]; then
      REPO_ROOT="$(pwd)"
      FOUND=yes
      break
    fi
    cd .. || break
  done
  cd "$CWD" || true
fi

if [[ -z "$REPO_ROOT" ]]; then
  echo "ERROR: Cannot detect repository root. Run this script from inside the repo or use a git repo."
  exit 1
fi

# resolve absolute module dir
MODULE_DIR="${REPO_ROOT}/${MODULE_REL}"
if [[ ! -d "${MODULE_DIR}" ]]; then
  echo "ERROR: Module directory not found: ${MODULE_DIR}"
  echo "Pass --module-dir <path> relative to repo root or verify path."
  exit 1
fi

# decide mvnw path
if [[ -n "${ROOT_MVNW_OVERRIDE}" ]]; then
  MVNW="${ROOT_MVNW_OVERRIDE}"
else
  MVNW="${REPO_ROOT}/mvnw"
fi
if [[ ! -x "${MVNW}" && ! -f "${MVNW}" ]]; then
  echo "ERROR: mvnw not found at ${MVNW}. Ensure mvnw exists and is executable in repo root."
  exit 1
fi

# logs dir default if not provided
if [[ -z "${LOGS_DIR}" ]]; then
  LOGS_DIR="${MODULE_DIR}/shard-results-logs"
fi

echo "Repo root: ${REPO_ROOT}"
echo "Module dir: ${MODULE_DIR}"
echo "Module basename: $(basename "${MODULE_DIR}")"
echo "Work base: ${WORK_BASE}"
echo "Logs dir: ${LOGS_DIR}"
echo "mvnw: ${MVNW}"
echo "Shards: ${N}"
echo

# prepare
rm -rf "${WORK_BASE}"* || true
mkdir -p "${WORK_BASE}"
mkdir -p "${LOGS_DIR}"

MODULE_BASENAME="$(basename "${MODULE_DIR}")"

echo "Creating per-shard working directories (using cp -a)..."
for i in $(seq 0 $((N-1))); do
  TARGET="${WORK_BASE}-${i}"
  echo "  -> ${TARGET}"
  rm -rf "${TARGET}"
  mkdir -p "${TARGET}"
  # copy module into target (cp -a will create ${TARGET}/${MODULE_BASENAME})
  cp -a "${MODULE_DIR}" "${TARGET}/"
  if [[ ! -d "${TARGET}/${MODULE_BASENAME}" ]]; then
    echo "ERROR: copy failed for shard ${i}; expected ${TARGET}/${MODULE_BASENAME}"
    exit 1
  fi
done
echo "Copies ready."
echo

# run shards in parallel
pids=()
for i in $(seq 0 $((N-1))); do
  (
    SHARD_MODULE_DIR="${WORK_BASE}-${i}/${MODULE_BASENAME}"
    cd "${SHARD_MODULE_DIR}" || exit 1

    LOGFILE="${LOGS_DIR}/shard-${i}.log"
    echo "===== START SHARD ${i} =====" | tee "${LOGFILE}"
    echo "Working dir: ${SHARD_MODULE_DIR}" | tee -a "${LOGFILE}"
    echo "Shard env: td-env-${i}" | tee -a "${LOGFILE}"
    echo "Invoking: ${MVNW} clean install -pl :${MODULE_BASENAME} -Pclearscape-tests -Dtest.totalShards=${N} -Dtest.shardIndex=${i} -Dtest.env=td-env-${i} -DskipITs=false" | tee -a "${LOGFILE}"

    # run mvn and stream to console + log
    "${MVNW}" clean install \
      -pl ":${MODULE_BASENAME}" \
      -Pclearscape-tests \
      -Dtest.totalShards="${N}" \
      -Dtest.shardIndex="${i}" \
      -Dtest.env="td-env-${i}" \
      -Dgit.commit.id.dotGitDirectory="${REPO_ROOT}/.git" \
      -DskipITs=false 2>&1 | tee -a "${LOGFILE}"

    echo "===== END SHARD ${i} =====" | tee -a "${LOGFILE}"
  ) &
  pids+=($!)
done

echo "Launched ${#pids[@]} shards. Waiting for completion..."
for pid in "${pids[@]}"; do
  wait "${pid}" || true
done

echo "All shards finished. Logs in: ${LOGS_DIR}"

