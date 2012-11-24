#!/bin/bash

SCALA="-XX:MaxPermSize=256m -Xms1G -Xmx1G -Xss8M"
java $SCALA  -jar `dirname $0`/sbt-launch.jar "$@"

