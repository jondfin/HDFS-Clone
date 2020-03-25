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
import ds.hdfs.hdfsProto.ClientQuery;
import ds.hdfs.hdfsProto.DataNodeResponse;

public class DataNode implements IDataNode
{
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
//    protected String MyName;
    protected int MyID;

    public DataNode(int id, String ip, int port)
    {
    	this.MyID = id;
    	this.MyIP = ip;
    	this.MyPort = port;
    	this.MyChunksFile = this.MyIP + "_chunks.txt";
    }

    public byte[] readBlock(byte[] inp)
    {
        try
        {
        }
        catch(Exception e)
        {
            System.out.println("Error at readBlock");
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    public byte[] writeBlock(byte[] inp)
    {
    	DataNodeResponse.Builder response = DataNodeResponse.newBuilder();

    	//Deserialize client message
    	ClientQuery query;
    	try{
    		query = ClientQuery.parseFrom(inp);
    	}catch(InvalidProtocolBufferException e) {
    		System.out.println("Error parsing client query");
    		e.printStackTrace();
    		response.setResponse(null);
    		response.setStatus(-1);
    		return null;
    	}
    	//Create file and write it
    	try {
	    	File f = new File(query.getFilename());
	    	f.createNewFile();
	    	FileOutputStream fos = new FileOutputStream(f);
	    	fos.write(query.getData());
	    	fos.close();
    	}catch(Exception e) {
    		System.out.println("Error writing file in Data Node");
    		e.printStackTrace();
    		response.setResponse(null);
    		response.setStatus(-1);
    		return null;
    	}
    	
    	//Let the client know that bytes were succesfully written
    	response.setResponse(null);
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
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
            System.setProperty("java.rmi.server.hostname", IP);
            Registry registry = LocateRegistry.getRegistry(Port);
            registry.rebind(Name, stub);
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
        //Set up name node
        NameNode nn = null;
		BufferedReader br = new BufferedReader(new FileReader("src/nn_config.txt"));
		String line = br.readLine();
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
        
        ArrayList<DataNode> DataNodes = new ArrayList<>();
        
        //Set up data nodes
        br = new BufferedReader(new FileReader("src/dn_config.txt"));
        line = br.readLine();
        while( (line = br.readLine()) != null) {
        	String parsedLine[] = line.split(";");
        	DataNodes.add(new DataNode(Integer.parseInt(parsedLine[0]), parsedLine[1], Integer.parseInt(parsedLine[2])));
        }
        br.close();
        
		//Bind to data nodes to server
        for(DataNode dn : DataNodes) {
        	boolean found = false;
        	while(!found){
				try{
					dn.BindServer(nn.name, nn.ip, nn.port);
					System.out.println("Bound DataNode: " + dn.MyID + " with address " + dn.MyIP + ":" + dn.MyPort);
					found = true;
				}catch(Exception e) {
					System.err.println("Couldn't connect to rmiregistry");
					TimeUnit.SECONDS.sleep(1);
				}
        	}
        }
        
    }
}
