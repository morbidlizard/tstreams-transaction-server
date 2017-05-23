#!/usr/bin/env bash
# It expects 1 argument:
# $1 path to config file

# Expects 2 arguments: property and value to set
#
# Returns: nothing
function changePropertyOfFile()
{
    local property=$1=
    local newValue=$2

    local record=`cat $1 | grep ${property}`

    `sed -i s:${record}:${property}${newValue}:g $1`
}

StoragePath='/storage'
Port=8080

changePropertyOfFile path ${StoragePath}
changePropertyOfFile port ${Port}