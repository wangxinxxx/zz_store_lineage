#!/usr/bin/env bash

build_runtime_classpath() {
  local repo_dir="$1"
  local mvn_bin="${MAVEN_BIN:-mvn}"
  local output_file="${repo_dir}/target/runtime-classpath.txt"
  local path_separator=":"

  case "$(uname -s)" in
    CYGWIN*|MINGW*|MSYS*)
      path_separator=";"
      ;;
  esac

  "${mvn_bin}" -q \
    -f "${repo_dir}/pom.xml" \
    -DincludeScope=test \
    -Dmdep.outputAbsoluteArtifactFilename=true \
    "-Dmdep.pathSeparator=${path_separator}" \
    "-Dmdep.outputFile=${output_file}" \
    org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath >/dev/null

  cat "${output_file}"
}
