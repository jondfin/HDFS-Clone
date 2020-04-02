Learnings:
	- Protobuf is way more versatile than I had noticed before. If done correctly I could easily
	serialize/deserialize entire data structures to/from files easily
	- The Name Node is crucial to HDFS, without it, HDFS cannot serve requests
		- Clients dont know where to write their data to or get their data from. The Name
			Node is responsible for establishing connection from the client to the Data Nodes
		- By handing Data Nodes the responsibility of reading/writing the actual data, Name Nodes
			are able to handle client requests easily
	- Service flow:
		Client --get file--> Name Node
			   <--block locations--
			---Read Block--> Data Node
			   <--block data----
	    Client --put file--> Name Node
	   		   <--data node location--
				---Write Blocks--> Data Node
	- DataNodes send heartbeats to join the system
		- Each data node has their own file to commit data to
	- A file becomes unavailable if the data nodes containing a file's specific set of blocks go down
		- A file has a set of blocks mapped to it by the Name node, this is kept in memory
			- The name node also has its own file to persist, but it only contains the block mappings	 