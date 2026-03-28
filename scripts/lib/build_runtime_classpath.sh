#!/usr/bin/env bash

build_runtime_classpath() {
  local repo_dir="$1"
  find \
    "${HOME}/.m2/repository" \
    "${repo_dir}/.m2-central" \
    "${repo_dir}/.deps" \
    -type f \
    -name '*.jar' \
    ! -name '*-sources.jar' \
    ! -path "${HOME}/.m2/repository/org/apache/spark/*/3.5.7/*" \
    ! -path "${HOME}/.m2/repository/org/apache/spark/*/3.1.2/*" \
    ! -path "${HOME}/.m2/repository/org/apache/thrift/libthrift/0.12.0/*" \
    ! -path "${HOME}/.m2/repository/org/apache/commons/commons-lang3/3.1/*" \
    ! -path "${HOME}/.m2/repository/org/apache/commons/commons-lang3/3.10/*" \
    ! -path "${HOME}/.m2/repository/org/apache/commons/commons-lang3/3.12.0/*" \
    ! -path "${HOME}/.m2/repository/org/apache/commons/commons-lang3/3.14.0/*" \
    ! -path "${HOME}/.m2/repository/commons-lang/commons-lang/2.1/*" \
    ! -path "${HOME}/.m2/repository/commons-lang/commons-lang/2.4/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-jdk14/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-log4j12/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.5.6/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.2/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.5/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.6/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.7/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.10/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.22/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.25/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.28/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/slf4j-api/1.7.30/*" \
    ! -path "${HOME}/.m2/repository/org/slf4j/jcl-over-slf4j/1.5.6/*" \
    ! -path "${HOME}/.m2/repository/Spark_Etl/*" \
    ! -path "${repo_dir}/.m2-central/org/slf4j/slf4j-api/1.7.25/*" \
    ! -path "${repo_dir}/.m2-central/commons-lang/commons-lang/2.4/*" \
    ! -path "${HOME}/.m2/repository/org/scala-lang/scala-library/2.12.0/*" \
    ! -path "${HOME}/.m2/repository/org/scala-lang/scala-library/2.12.8/*" \
    ! -path "${HOME}/.m2/repository/org/scala-lang/scala-library/2.12.12/*" \
    ! -path "${HOME}/.m2/repository/org/scala-lang/scala-reflect/2.12.8/*" \
    ! -path "${HOME}/.m2/repository/org/scala-lang/modules/scala-xml_2.12/1.0.6/*" \
    ! -path "${HOME}/.m2/repository/com/squareup/okhttp/okhttp/2.7.5/*" \
    ! -path "${HOME}/.m2/repository/com/squareup/okio/okio/1.6.0/*" \
    | sort \
    | paste -sd: -
}
