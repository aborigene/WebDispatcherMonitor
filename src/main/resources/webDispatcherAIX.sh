#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $DIR
JAVA_HOME="$DIR/../../jre"
cd $DIR
$JAVA_HOME/bin/java -jar webdispatcherExtension.jar