LIB_PATH=lib/protobuf-java-3.11.4.jar
all: clean
	mkdir bin
	javac -classpath $(LIB_PATH) -d bin/ keyValue/KeyValue.java  keyValue/client/* keyValue/util/* keyValue/replicaServers/*
clean:
	rm -rf bin writeAheadLog
