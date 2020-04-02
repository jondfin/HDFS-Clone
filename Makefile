all:
	javac -d bin/ -cp src/com/google/protobuf/protobuf.jar -sourcepath src/ds/hdfs/ src/ds/hdfs/*.java
clean:
	rm -rf bin/ds
	rm *_chunks.txt
	rm src/NNMD.txt
