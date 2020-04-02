all:
	javac -d bin/ -cp src/com/google/protobuf/protobuf.jar -sourcepath src/ds/hdfs/ src/ds/hdfs/*.java

nn:
	java -cp src/com/google/protobuf/protobuf.jar:bin/ ds.hdfs.NameNode
dn:
	java -cp src/com/google/protobuf/protobuf.jar:bin/ ds.hdfs.DataNode
client:
	java -cp src/com/google/protobuf/protobuf.jar:bin/ ds.hdfs.Client

clean:
	rm -rf bin/ds
	rm *_chunks.txt
	rm src/NNMD.txt
