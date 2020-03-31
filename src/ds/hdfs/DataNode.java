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

import ds.hdfs.IDataNode.*;
import ds.hdfs.hdfsProto.Block;
import ds.hdfs.hdfsProto.ClientQuery;
import ds.hdfs.hdfsProto.DataNodeData;
import ds.hdfs.hdfsProto.DataNodeResponse;
import ds.hdfs.hdfsProto.NameNodeData;

public class DataNode implements IDataNode
{
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
//    protected String MyName;
    protected int MyID;
    
    private static final ByteString ERROR_MSG = ByteString.copyFrom("ERROR".getBytes());

    public DataNode(int id, String ip, int port)
    {
    	this.MyID = id;
    	this.MyIP = ip;
    	this.MyPort = port;
    	this.MyChunksFile = "DN" + this.MyID + "_chunks.txt";
    }

    /**
     * Receives block number to retrieve
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
    		response.setResponse(ERROR_MSG);
    		response.setStatus(-1);
    		return response.build().toByteArray();
    	}
    	int blockNum = requestedBlock.getBlocknum();
//    	ByteString data = requestedBlock.getData(); //TODO temp can remove later
    	
    	System.out.println("Client requested block " + blockNum);
    	
    	//Open chunk file to read data
    	try {
    		//Deserialize line of data
    		FileInputStream fis = new FileInputStream(this.MyChunksFile);
			DataNodeData dsData = DataNodeData.parseFrom(fis);
			for(Block b : dsData.getBlockList()) {
				if(b.getBlocknum() == blockNum) {
    				//Found requested block
    				response.setResponse(b.getData());
    				response.setStatus(1);
    				fis.close();
    				return response.build().toByteArray();
    			}
			}
    		fis.close();
    		System.out.println("Could not find block " + blockNum);
    		response.setResponse(ERROR_MSG);
    		response.setStatus(-1);
            return response.build().toByteArray();
    	}catch(FileNotFoundException e) {
    		System.out.println("Could not find " + this.MyChunksFile);
    		response.setResponse(ERROR_MSG);
    		response.setStatus(-1);
    		return response.build().toByteArray();
    	} catch (IOException e) {
    		System.out.println("Could not read block: IOException");
    		response.setResponse(ERROR_MSG);
    		response.setStatus(-1);
    		return response.build().toByteArray();
		}
    }

    /**
     * Receives the block number to write and the data associated with it
     */
    public byte[] writeBlock(byte[] inp)
    {
    	DataNodeResponse.Builder response = DataNodeResponse.newBuilder();

    	//Deserialize client message
    	Block blockToWrite;
    	try{
    		blockToWrite = Block.parseFrom(inp);
    	}catch(InvalidProtocolBufferException e) {
    		System.out.println("Error parsing client query in writeBlock");
    		e.printStackTrace();
    		response.setResponse(ERROR_MSG);
    		response.setStatus(-1);
    		return response.build().toByteArray();
    	}

    	System.out.println(blockToWrite.getData().toStringUtf8());
    	
    	//Serialize data
    	DataNodeData.Builder serializedData = DataNodeData.newBuilder();
    	serializedData.addBlock(blockToWrite);
    	
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
    		response.setResponse(ERROR_MSG);
    		response.setStatus(-1);
    		return response.build().toByteArray();
    	}
    	
    	//Let the client know that bytes were succesfully written
//    	response.setResponse(ERROR_MSG);
    	response.setStatus(1);
        return response.build().toByteArray();
    }

    public void BlockReport() throws IOException
    {
    }

    public void BindServer(String Name, String IP, int Port)
    {
        try
        {
        	//Create local registry on localhost
        	LocateRegistry.createRegistry(Port);
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

    public INameNode GetNNStub(String Name, String IP, int Port)
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
        NameNode nn = null;
		BufferedReader br = new BufferedReader(new FileReader("src/nn_config.txt"));
		String line = br.readLine();
		line = br.readLine(); //skip over block size
		while( (line = br.readLine()) != null) {
			String parsedLine[] = line.split(";");
			//Create new name node
			nn = new NameNode(parsedLine[1], Integer.parseInt(parsedLine[2]), parsedLine[0]);
			System.out.println("Found Name Node: \n\t" + parsedLine[0] + ": " + parsedLine[1] + " Port = " + Integer.parseInt(parsedLine[2]));
		}
		br.close();
		
		//Enable services
//		System.setProperty("java.rmi.server.hostname" , "localhost");
		System.setProperty("java.security.policy","src/permission.policy");

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        
        //Set up data node
        DataNode dn = null;
        br = new BufferedReader(new FileReader("src/dn_config.txt"));
        line = br.readLine();
        while( (line = br.readLine()) != null) {
        	String parsedLine[] = line.split(";");
        	dn = new DataNode(Integer.parseInt(parsedLine[0]), parsedLine[1], Integer.parseInt(parsedLine[2]));
        }
        br.close();
        
		//Bind to data nodes to server
		dn.BindServer(String.valueOf(dn.MyID), dn.MyIP, dn.MyPort);
        
    	//TODO GetNNStub to send heartbeats
    }
}
