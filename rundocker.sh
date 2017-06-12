#!/usr/bin/env bash
# It expects 3 argument:
# $1 path to config file
# $2 transaction server version
# $3 sfl4j version

EXTERNAL_CONFIG_PATH=$1

CONFIG='/tmp/config.properties'
PROJECT_PATH='/opt/bin/tts/'

cp ${EXTERNAL_CONFIG_PATH} ${CONFIG}

# todo: fixit, why do you assign +X attribute to $CONFIG?
chmod +x ${CONFIG}

# todo: test script. I removed backticks(``) from places it is excessive. Don't use `` if you don't understand what it does
#

# Expects 2 arguments: property and value to set
#
# Returns: nothing
function change_property()
{
    local PROPERTY="$1="
    local NEW_VALUE=$2
    local RECORD=$(grep ${PROPERTY} ${CONFIG})
    sed -i s:${RECORD}:${PROPERTY}${NEW_VALUE}:g ${CONFIG}
}

change_property 'storage-model.file-prefix' '/storage'
change_property 'bind.port' 8080
# todo: what about bind.host parameter? I suppose it must be 127.0.0.1 always, but an user can change it.
# todo: fixit
#

exec java -Dconfig=${CONFIG} \
  -classpath ${PROJECT_PATH}tstreams-transaction-server-${TTS_VERSION}.jar:${PROJECT_PATH}slf4j-api-${SLF4J_VERSION}.jar:${PROJECT_PATH}slf4j-log4j12-${SLF4J_VERSION}.jar \
  com.bwsw.tstreamstransactionserver.ServerLauncher
