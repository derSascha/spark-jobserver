#!/bin/bash
# Script to start the job server
# Extra arguments will be spark-submit options, for example
#  ./server_start.sh --jars cassandra-spark-connector.jar
#
# Environment vars (note settings.sh overrides):
#   JOBSERVER_MEMORY - defaults to 1G, the amount of memory (eg 512m, 2G) to give to job server
#   JOBSERVER_CONFIG - alternate configuration file to use
#   JOBSERVER_FG    - launches job server in foreground; defaults to forking in background
set -e

get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

. $appdir/setenv.sh

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$appdir/gc.out
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY \
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

# To truly enable JMX in AWS and other containerized environments, also need to set
# -Djava.rmi.server.hostname equal to the hostname in that environment.  This is specific
# depending on AWS vs GCE etc.
# JAVA_OPTS="${JAVA_OPTS} \
#            -Dcom.sun.management.jmxremote.port=$JMX_PORT \
#            -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
#            -Dcom.sun.management.jmxremote.authenticate=false \
#            -Dcom.sun.management.jmxremote.ssl=false"

MAIN="spark.jobserver.JobServer"

PIDFILE=$appdir/spark-jobserver.pid
if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE") >/dev/null 2>&1 ; then
   echo 'Job server is already running'
   exit 1
fi

buildVersionFile="$appdir/build.version"
if [ -f "$buildVersionFile" ] ; then
  echo "BUILD-VERSION: $(cat $buildVersionFile)" >>"$LOG_DIR/spark-job-server.log"
fi

# Start Kerberos ticket renewer if necessary
if [ -n "$JOBSERVER_KEYTAB" ] ; then
  "$appdir"/kerberos-ticket-renewer.sh 2>&1 >>"$LOG_DIR"/kerberos-ticket-renewer.log </dev/null &
fi

cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
  --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
  $SPARK_SUBMIT_OPTIONS
  --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $SPARK_SUBMIT_JAVA_OPTIONS"
  $@ $appdir/spark-job-server.jar $conffile'
if [ -z "$JOBSERVER_FG" ]; then
  eval $cmd > $LOG_DIR/server_start.log 2>&1 < /dev/null &
  echo $! > $PIDFILE
else
  eval $cmd
fi
