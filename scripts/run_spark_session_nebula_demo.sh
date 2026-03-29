#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/lib/build_runtime_classpath.sh"

DEFAULT_JAVA8="/Users/zz/Library/Java/JavaVirtualMachines/corretto-1.8.0_482/Contents/Home/bin/java"
DEFAULT_JBR="/Applications/IntelliJ IDEA.app/Contents/jbr/Contents/Home/bin/java"

JAVA_BIN="${JAVA_BIN:-java}"
if ! command -v "${JAVA_BIN}" >/dev/null 2>&1; then
  if [[ -x "${DEFAULT_JAVA8}" ]]; then
    JAVA_BIN="${DEFAULT_JAVA8}"
  else
    JAVA_BIN="${DEFAULT_JBR}"
  fi
fi

MAVEN_BIN="${MAVEN_BIN:-mvn}"

cd "${REPO_DIR}"

"${MAVEN_BIN}" -q test-compile

JAR_CP="$(build_runtime_classpath "${REPO_DIR}")"

JAVA_VERSION_OUTPUT="$("${JAVA_BIN}" -version 2>&1 | head -n 1)"
if [[ "${JAVA_VERSION_OUTPUT}" != *"1.8."* ]]; then
  exec "${JAVA_BIN}" \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    -Dio.netty.tryReflectionSetAccessible=true \
    -cp "target/test-classes:target/classes:${JAR_CP}" \
    com.zhuanzhuan.lineage.demo.SparkSessionNebulaDemo \
    "$@"
fi

exec "${JAVA_BIN}" \
  -Dio.netty.tryReflectionSetAccessible=true \
  -cp "target/test-classes:target/classes:${JAR_CP}" \
  com.zhuanzhuan.lineage.demo.SparkSessionNebulaDemo \
  "$@"
