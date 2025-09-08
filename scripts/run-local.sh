#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")"/.. && pwd)"
JAR_PATTERN="${ROOT_DIR}/target/*-shaded.jar"

if ! ls ${JAR_PATTERN} >/dev/null 2>&1; then
  echo "Shaded jar not found. Run: mvn -q -DskipTests package" >&2
  exit 1
fi

JAR=$(ls ${JAR_PATTERN} | head -n1)

if command -v spark-submit >/dev/null 2>&1; then
  spark-submit --class bike.rapido.dq.EntryPoint "${JAR}" \
    --demo --schema default --tables dummy_table --mode all
  exit $?
fi

# Try a best-effort fallback without spark-submit by assembling a Spark classpath
echo "spark-submit not found; attempting java -cp fallback" >&2

find_spark_jars_dir() {
  if [ -n "${SPARK_HOME:-}" ] && [ -d "$SPARK_HOME/jars" ]; then
    echo "$SPARK_HOME/jars"; return 0
  fi
  for base in \
    "/usr/local/opt/apache-spark/libexec/jars" \
    "/opt/homebrew/opt/apache-spark/libexec/jars" \
    "/usr/local/Cellar/apache-spark" \
    "/opt/homebrew/Cellar/apache-spark"
  do
    if [ -d "$base" ]; then
      if [ -d "$base/libexec/jars" ]; then
        echo "$base/libexec/jars"; return 0
      fi
      latest=$(ls -1dt "$base"/* 2>/dev/null | head -n1 || true)
      if [ -n "$latest" ] && [ -d "$latest/libexec/jars" ]; then
        echo "$latest/libexec/jars"; return 0
      fi
    fi
  done
  return 1
}

SPARK_JARS_DIR="$(find_spark_jars_dir || true)"
if [ -z "$SPARK_JARS_DIR" ]; then
  echo "Unable to locate Spark jars. Please install Spark or set SPARK_HOME, or run with spark-submit." >&2
  echo "Tried SPARK_HOME/jars and common Homebrew locations." >&2
  exit 1
fi

CP="${JAR}:${SPARK_JARS_DIR}/*"
exec java -Xms512m -Xmx2g -cp "$CP" bike.rapido.dq.EntryPoint \
  --demo \
  --schema default \
  --tables dummy_table \
  --mode all
