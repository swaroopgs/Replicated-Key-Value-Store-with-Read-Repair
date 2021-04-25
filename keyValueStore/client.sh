#!/bin/bash +vx
LIB_PATH=$"lib/protobuf-java-3.11.4.jar"
#port
java -classpath bin:$LIB_PATH keyValueStore.client.Client $1 $2
