all:
	javac -d bin/ -cp src/com/google/protobuf/protobuf.jar -sourcepath src/ds/hdfs/ src/ds/hdfs/*.java

startNN:
	java -cp src/com/google/protobuf/protobuf.jar:bin/ ds.hdfs.NameNode
startDN:
	java -cp src/com/google/protobuf/protobuf.jar:bin/ ds.hdfs.DataNode
startClient:
	java -cp src/com/google/protobuf/protobuf.jar:bin/ ds.hdfs.Client

clean:
	rm -rf bin/ds
	rm *_chunks.txt
	rm src/NNMD.txt
