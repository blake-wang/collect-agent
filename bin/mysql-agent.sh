#!/usr/bin/env bash

LOG_FILE_NAME=mysql-agent

IS_SPECIAL=0
if [ $# != 0 ]; then
  IS_SPECIAL=1
fi

BinPath=$(cd "$(dirname "$0")"; pwd);

error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

export JAVA_HOME=/usr/java/jdk1.8.0_131
export JAVA="$JAVA_HOME/bin/java"

CLASS_PATH="${BinPath}/../conf"

JAVA_OPT="${JAVA_OPT} -server -Xms128m -Xmx128m -Xmn64m"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BinPath}/../lib"
JAVA_OPT="${JAVA_OPT} -Dlog.file.name=${LOG_FILE_NAME}"
JAVA_OPT="${JAVA_OPT} -cp ${CLASS_PATH}"

if [ ${IS_SPECIAL} -eq 0 ]; then
  ${JAVA} ${JAVA_OPT} com.ijunhai.rocketmq.mysql.Replicator -c ${CLASS_PATH}/mysql-agent.properties -l ${CLASS_PATH}/logback.xml
else
  ${JAVA} ${JAVA_OPT} com.ijunhai.rocketmq.mysql.Replicator $@
fi