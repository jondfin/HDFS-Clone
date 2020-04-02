//Written By Shaleen Garg
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.nio.charset.Charset;

import ds.hdfs.hdfsProto.Block;
import ds.hdfs.hdfsProto.NodeBlocks;
import ds.hdfs.hdfsProto.NodeData;
import ds.hdfs.hdfsProto.DataNodeResponse;
import ds.hdfs.hdfsProto.HeartBeat;
import ds.hdfs.hdfsProto.NameNodeResponse;

public class DataNode implements IDataNode
{
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
    protected int MyID;
    private static TimerTask heartBeat;
    private Timer timer;
    
    private static int interval = 5000; //Measured in milliseconds. Default 5 seconds

    public DataNode(int id, String ip, int port)
    {
    	this.MyID = id;
    	this.MyIP = ip;
    	this.MyPort = port;
    	this.MyChunksFile = "DN" + this.MyID + "_chunks.txt";
    }

    /**
     * Receives filename and block number to retrieve
     */
    public byte[] readBlock(byte[] inp)
    {
	    	DataNodeResponse.Builder response = DataNodeResponse.newBuilder();
	    	
	    	//Deserialize client message
	    	Block requestedBlock;
	    	try {
	    		requestedBlock = Block.parseFrom(inp);
	    	}catch(InvalidProtocolBufferException e) {
	    		System.out.println("Error parsing client query in readBlock");
	    		e.printStackTrace();
	    		response.setStatus(-1);
	    		return response.build().toByteArray();
	    	}
	    	int blockNum = requestedBlock.getBlocknum();
	    	
	    	//Open chunk file to read data
	    	try {
	    		//Deserialize line of data
	    		FileInputStream fis = new FileInputStream(this.MyChunksFile);
				NodeData dsData = NodeData.parseFrom(fis);
				//Look for filename
				for(NodeBlocks dnb : dsData.getDataList()) {
					//Find and send requested block
					for(Block b : dnb.getBlockList()) {
						if(b.getBlocknum() == blockNum) {
		    				//Found requested block
		    				response.setResponse(b.getData());
		    				response.setStatus(1);
		    				fis.close();
		    				return response.build().toByteArray();
		    			}
					}
				}
	    		fis.close();
	    		System.out.println("Could not find block " + blockNum);
	    		response.setStatus(-1);
	            return response.build().toByteArray();
	    	}catch(FileNotFoundException e) {
	    		System.out.println("Could not find " + this.MyChunksFile);
	    		response.setStatus(-1);
	    		return response.build().toByteArray();
	    	} catch (IOException e) {
	    		System.out.println("Could not read block: IOException");
	    		response.setStatus(-1);
	    		return response.build().toByteArray();
			}
    }

    /**
     * Receives the block number to write and the data associated with it
     */
    public byte[] writeBlock(byte[] inp)
    {
    	synchronized(this) {
	    	DataNodeResponse.Builder response = DataNodeResponse.newBuilder();
	
	    	//Deserialize client message
	    	Block blockToWrite;
	    	try{
	    		blockToWrite = Block.parseFrom(inp);
	    	}catch(InvalidProtocolBufferException e) {
	    		System.out.println("Error parsing client query in writeBlock");
	    		e.printStackTrace();
	    		response.setStatus(-1);
	    		return response.build().toByteArray();
	    	}
	
	    	//Serialize data
	    	NodeBlocks.Builder block = NodeBlocks.newBuilder();
	    	block.addBlock(blockToWrite);
	    	NodeData.Builder serializedData = NodeData.newBuilder();
	    	serializedData.addData(block);
	    	
	    	//Write to blockfile
	    	try {
		    	File f = new File(this.MyChunksFile);
		    	if(f.exists() == false) f.createNewFile();
		    	FileOutputStream fos = new FileOutputStream(f, true);
		    	fos.write(serializedData.build().toByteArray());
		    	fos.close();
	    	}catch(Exception e) {
	    		System.out.println("Error writing file in Data Node");
	    		e.printStackTrace();
	    		response.setStatus(-1);
	    		return response.build().toByteArray();
	    	}
	    	
	    	//Let the client know that bytes were succesfully written
	    	response.setStatus(1);
	        return response.build().toByteArray();
    	}
    }


    public void BindServer(String Name, String IP, int Port)
    {
        try
        {
        	//Create local registry on localhost
//        	LocateRegistry.createRegistry(Port);
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
            System.setProperty("java.rmi.server.hostname", IP);
            Registry registry = LocateRegistry.getRegistry(Port);
            System.out.println(registry);
            boolean found = false;
            while(!found) {
            	try{
            		registry.rebind(Name, stub);
            		found = true;
            	}catch(Exception r) {
            		System.err.println("Couldn't connect to rmiregistry");
            		System.err.println("Attempting connection again...");
    				TimeUnit.SECONDS.sleep(1);
            	}
            }
            System.out.println("\nDataNode connected to RMIregistry\n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
        }
    }
    

    public static INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("NameNode Found!");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }

    public static void main(String args[]) throws InvalidProtocolBufferException, IOException, InterruptedException
    {
        //Get up name node
		BufferedReader br = new BufferedReader(new FileReader("src/nn_config.txt"));
		String line = br.readLine(); //skip first line
		line = br.readLine(); //skip over block size
		line = br.readLine(); //skip over timeout
		line = br.readLine(); //skip over replication factor
		line = br.readLine();
		String parsedLine[] = line.split(";");
		//Create new name node
		final NameNode nn = new NameNode(parsedLine[1], Integer.parseInt(parsedLine[2]), parsedLine[0]);
		System.out.println("Found Name Node: \n\t" + parsedLine[0] + ": " + parsedLine[1] + " Port = " + Integer.parseInt(parsedLine[2]));
		br.close();
		
		//Enable services
//		System.setProperty("java.rmi.server.hostname" , "localhost");
		System.setProperty("java.security.policy","src/permission.policy");

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        
        //Set up data nodes
        br = new BufferedReader(new FileReader("src/dn_config.txt"));
        line = br.readLine();
        line = br.readLine(); //skip first line
        line = br.readLine(); //read heartbeat timeout interval
        interval = Integer.parseInt(line.split("=")[1].trim());
        while( (line = br.readLine()) != null) {
        	String parsedLine2[] = line.split(";");
        	final DataNode dn = new DataNode(Integer.parseInt(parsedLine2[0]), parsedLine2[1], Integer.parseInt(parsedLine2[2]));
        	//Get chunk file
        	File chunkFile = new File(dn.MyChunksFile);
        	if(chunkFile.exists() == false) chunkFile.createNewFile();
        	//Bind to data nodes to server
    		dn.BindServer(String.valueOf(dn.MyID), dn.MyIP, dn.MyPort);
    		//Schedule heartbeats
    		dn.heartBeat = new TimerTask() {
    			@Override
    			public void run() {
    				System.out.println("Sending heartbeat to " + nn.ip + ":" + nn.port + " from " + dn.MyIP + ":" + dn.MyPort);
    				INameNode stub = GetNNStub(nn.name, nn.ip, nn.port);
    				//Send blocks to Name Node
    				try {
    					//Get chunk file
    		        	File chunkFile = new File(dn.MyChunksFile);
    		        	if(chunkFile.exists() == false) chunkFile.createNewFile();
    		        	if(chunkFile.length() != 0) {
		    				FileInputStream fis = new FileInputStream(chunkFile);
		    				NodeData storedData = NodeData.parseFrom(fis);
		    				
		    				//Get the data and serialize it
		    				HeartBeat.Builder hb = HeartBeat.newBuilder();
		    				String nodeInfo = dn.MyID + ";" + dn.MyIP + ";" + dn.MyPort;
		    				hb.setNodeinfo(nodeInfo);
		    				NodeData.Builder fileList = NodeData.newBuilder();
		    				//Go through each data entry
		    				for(NodeBlocks dnb : storedData.getDataList()) {
	    						NodeBlocks.Builder blocks = NodeBlocks.newBuilder();
	    						blocks.setFilename(dnb.getFilename());
	    						for(Block b : dnb.getBlockList()) {
	    							//Recreating the block but without the data
	    							Block.Builder bs = Block.newBuilder();
	    							bs.setBlocknum(b.getBlocknum());
	    							blocks.addBlock(bs);
	    						}
	    						fileList.addData(blocks);
		    				}
		    				fis.close();
		    				hb.setData(fileList);
		    				NameNodeResponse nnr = NameNodeResponse.parseFrom(stub.heartBeat(hb.build().toByteArray()));
		    				if(nnr.getStatus() == -1) {
		    					System.out.println("Namenode had an error processing the heartbeat");
		    					return;
		    				}
    		        	}else {
		    				HeartBeat.Builder hb = HeartBeat.newBuilder();
		    				String nodeInfo = dn.MyID + ";" + dn.MyIP + ";" + dn.MyPort;
		    				hb.setNodeinfo(nodeInfo);
		    				hb.setData(NodeData.newBuilder());
		    				NameNodeResponse nnr = NameNodeResponse.parseFrom(stub.heartBeat(hb.build().toByteArray()));
		    				if(nnr.getStatus() == -1) {
		    					System.out.println("Namenode had an error processing the heartbeat");
		    					return;
		    				}
    		        	}
    				}catch(FileNotFoundException e) {
    					e.printStackTrace();
    				} catch (IOException e) {
						e.printStackTrace();
					}
    			}
    		};
    		dn.timer = new Timer();
    		dn.timer.scheduleAtFixedRate(heartBeat, interval, interval);
    		//Add to list
        }
        br.close();
    }
}
