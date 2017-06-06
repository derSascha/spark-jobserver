#!/bin/bash -ue
# Script for packaging all the job server files to .tar.gz for Mesos or other single-image deploys

if [ "$#" -ne 1 ]; then
  echo "Syntax: ${0} <Environment>"
  echo "   for a list of environments, ls config/*.sh"
  exit 0
fi

ENV="${1}"
bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}"; pwd)

if [ -z "${CONFIG_DIR:-}" ]; then
  CONFIG_DIR=$(cd "${bin}/../config/"; pwd)
fi
configFile="${CONFIG_DIR}/${ENV}.sh"

if [ ! -f "${configFile}" ]; then
  echo "Could not find ${configFile}"
  exit 1
fi
# shellcheck source=/dev/null
. "${configFile}"

majorRegex='([0-9]+\.[0-9]+)\.[0-9]+'
set +u
if [[ "${SCALA_VERSION}" =~ ${majorRegex} ]]
then
  majorVersion="${BASH_REMATCH[1]}"
else
  echo "Please specify SCALA_VERSION in ${configFile}"
  exit 1
fi
set -u

WORK_DIR=$(mktemp -d)
FILE_DIR="$WORK_DIR/spark-job-server-${ENV}"
TAR_FILE="spark-job-server-${ENV}.tar.gz"

function finish {
    rm -Rf "${WORK_DIR}"
}
trap finish EXIT

echo "Packaging job-server for environment ${ENV}..."

pushd "${bin}/.." > /dev/null
  if ! sbt ++"${SCALA_VERSION}" job-server-extras/assembly; then
    echo "Assembly failed"
    exit 1
  fi

  FILES="job-server-extras/target/scala-$majorVersion/spark-job-server.jar
         bin/server_start.sh
         bin/server_stop.sh
         bin/kerberos-ticket-renewer.sh
         bin/manager_start.sh
         bin/setenv.sh
         ${CONFIG_DIR}/${ENV}.conf
         config/shiro.ini
         config/log4j-server.properties"

  mkdir -p "$FILE_DIR"
  cp $FILES "$FILE_DIR"/
  cp "$configFile" "$FILE_DIR"/settings.sh
popd > /dev/null

rm -f "${TAR_FILE}"
tar zcvf "$TAR_FILE" -C "$WORK_DIR" "spark-job-server-${ENV}/"

echo "Created distribution at ${TAR_FILE}"
