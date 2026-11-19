#!/bin/sh
set -eu
BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
MAVEN_VERSION=3.9.9
MAVEN_HOME_DIR="${MAVEN_USER_HOME:-${HOME}/.m2}/wrapper/dists/apache-maven-${MAVEN_VERSION}"
MAVEN_BIN="${MAVEN_HOME_DIR}/apache-maven-${MAVEN_VERSION}/bin/mvn"
if [ ! -x "$MAVEN_BIN" ]; then
  mkdir -p "$MAVEN_HOME_DIR"
  ARCHIVE="$MAVEN_HOME_DIR/apache-maven-${MAVEN_VERSION}-bin.zip"
  URL="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.zip"
  if command -v curl >/dev/null 2>&1; then curl -fsSL "$URL" -o "$ARCHIVE"
  elif command -v wget >/dev/null 2>&1; then wget -q "$URL" -O "$ARCHIVE"
  else echo "ERROR: curl or wget is required to bootstrap Maven." >&2; exit 1
  fi
  command -v unzip >/dev/null 2>&1 || { echo "ERROR: unzip is required to bootstrap Maven." >&2; exit 1; }
  unzip -q -o "$ARCHIVE" -d "$MAVEN_HOME_DIR"
  rm -f "$ARCHIVE"
fi
exec "$MAVEN_BIN" -f "$BASE_DIR/pom.xml" "$@"
