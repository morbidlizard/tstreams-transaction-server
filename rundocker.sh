#!/usr/bin/env bash
# It expects 3 argument:
# $1 path to config file
# $2 transaction server version
# $3 lsf4j version


config='/tmp/config.properties'
`cp $1 ${config}`
`chmod +x ${config}`

# Expects 2 arguments: property and value to set
#
# Returns: nothing
function changePropertyOfFile()
{
    local property=$1=
    local newValue=$2

    local record=`cat ${config} | grep ${property}`

    `sed -i s:${record}:${property}${newValue}:g ${config}`
}

changePropertyOfFile path '/storage'
changePropertyOfFile port 8080

pathToProject='/opt/bin/tts/'


`java -Dconfig=${config} \
  -classpath ${pathToProject}tstreams-transaction-server-${2}.jar:${pathToProject}slf4j-api-${3}.jar:${pathToProject}slf4j-log4j12-${3}.jar \
  com.bwsw.tstreamstransactionserver.ServerLauncher`