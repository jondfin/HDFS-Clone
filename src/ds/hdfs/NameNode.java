package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import ds.hdfs.hdfsProto.Block;
import ds.hdfs.hdfsProto.ClientQuery;
import ds.hdfs.hdfsProto.DataNodeBlocks;
import ds.hdfs.hdfsProto.DataNodeData;
import ds.hdfs.hdfsProto.DataNodeResponse;
import ds.hdfs.hdfsProto.HeartBeat;
import ds.hdfs.hdfsProto.NameNodeData;
import ds.hdfs.hdfsProto.NameNodeResponse;

import com.google.protobuf.*;

public class NameNode implements INameNode{

	//Holds the list of files and their meta-data
	private static ArrayList<FileInfo> fileList = new ArrayList<>();
	//Keep track of what blocks are being used
	private static BitSet bitset = new BitSet(Integer.MAX_VALUE); //might be too big 
	private static ArrayList<DataNode> dataNodes = new ArrayList<>();
	
	private static final ByteString ERROR_MSG = ByteString.copyFrom("ERROR".getBytes());
	
	private static int timeout = 10000;  //Measured in milliseconds. Default 10 seconds
	
	protected Registry serverRegistry;
	
	String ip;
	int port;
	String name;
	
	public NameNode(String addr,int p, String nn)
	{
		this.ip = addr;
		this.port = p;
		this.name = nn;
	}
	
	public static class DataNode
	{
		String ip;
		int port;
		String serverName;
		ArrayList<Integer> blocks;
		boolean alive;
		public DataNode(String addr,int p,String sname)
		{
			this.ip = addr;
			this.port = p;
			this.serverName = sname;
			this.blocks = new ArrayList<>();
			this.alive = false;
		}
		
		@Override
		public String toString() {
			return this.ip + ";" + this.port + ";" + this.serverName;
		}
	}
	
	public static class FileInfo
	{
		String filename;
//		int filehandle;
//		boolean writemode;
		ArrayList<Integer> Chunks;
		
//		public FileInfo(String name, int handle, boolean option)
		public FileInfo(String name)
		{
			this.filename = name;
//			filehandle = handle;
//			writemode = option;
			this.Chunks = new ArrayList<>();
		}
	}
	
	/**
	 * Name Node checks if it has the file
	 * @param Name of the file to be searched for
	 * @return FileInfo if it exists, null otherwise
	 */
	private FileInfo findInFilelist(String filename)
	{
		System.out.println("Looking for " + filename);
		for(FileInfo file : fileList) {
			if(file.filename.equals(filename)) {
				System.out.println(file);
				return file;
			}
		}
		return null;
	}
	
	public byte[] openFile(byte[] inp) throws RemoteException
	{
		DataNodeResponse.Builder response = DataNodeResponse.newBuilder();
		try
		{
		}
		catch (Exception e) 
		{
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	public byte[] closeFile(byte[] inp ) throws RemoteException
	{
		DataNodeResponse.Builder response = DataNodeResponse.newBuilder();
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at closefileRequest " + e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		
		return response.build().toByteArray();
	}
	/**
	 * Input is a byte array that contains the filename
	 * Returns 1 and block locations if the file exists, 0 if the file does not exist, -1 for error
	 */
	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		NameNodeResponse.Builder response = NameNodeResponse.newBuilder();
		
		//Deserialize client message
		ClientQuery query;
		try {
			query = ClientQuery.parseFrom(inp);
		} catch (InvalidProtocolBufferException e) {
			System.out.println("Error parsing client query");
			e.printStackTrace();
			response.setResponse(ERROR_MSG);
    		response.setStatus(-1);
    		return response.build().toByteArray();
		}
		String filename = query.getFilename();
		
		//Check if file exists
		FileInfo f = findInFilelist(filename);
		if(f == null) {
			System.out.println("File does not exist in HDFS!");
			response.setStatus(0); //OK if the client wants to PUT file
			return response.build().toByteArray();
		}
		
		//File exists, can return block locations
		String blockLocations = "";
		ArrayList<Integer> blocks = new ArrayList<>();
		int count = 0;
		for(Integer i : f.Chunks) {
			blockLocations = blockLocations.concat(i.toString());
			blocks.add(i);
			if(count < f.Chunks.size() - 1) blockLocations = blockLocations.concat(",");
			count++;
		}
		//Check data nodes
		for(DataNode d : dataNodes) {
			if(d.alive == true && d.blocks.containsAll(blocks)) {
				String msg = d.toString() + ";" + blockLocations; 
				response.setResponse(ByteString.copyFrom(msg.getBytes()));
				response.setStatus(1); //OK
				return response.build().toByteArray();
			}
		}
		System.out.println("DataNode containing " + filename + " is down!");
		response.setResponse(ERROR_MSG);
		response.setStatus(-1);
		return response.build().toByteArray();
	}
	
	/**
	 * Input is the filename
	 * Returns an available block number and the info of the data node to write to
	 */
	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		NameNodeResponse.Builder response = NameNodeResponse.newBuilder();
		
		//Deserialize message
		try {
			ClientQuery query = ClientQuery.parseFrom(inp);
			int available = bitset.nextClearBit(0);
			if(bitset.cardinality() == Integer.MAX_VALUE) {
				System.out.println("Error assigning blocks: No more blocks left!");
				response.setResponse(ERROR_MSG);
				response.setStatus(-1);
				return response.build().toByteArray();
			}
			//Block is available, map it and send it to client
			//TODO remove since this is used for appending data
			boolean found = false;
			String filename = query.getFilename();
			for(FileInfo f : fileList) {
				if(f.filename.equals(filename)) {
					//Mark chunk as taken and add it to the file's chunklist
					bitset.set(available);
					f.Chunks.add(Integer.valueOf(available));
					found = true;
					break;
				}
			}
			if(found == false) {
				System.out.println("Adding new file: " + filename);
				FileInfo newFile = new FileInfo(filename);
				//Mark chunk as taken and add it to the file's chunklist
				bitset.set(available);
				newFile.Chunks.add(Integer.valueOf(available));
				fileList.add(newFile);
			}
			//Contains the block number to write and the data node holding that block
			String s = available + ";" + dataNodes.get(0).toString(); //TODO replication factor and choosing a different data node
			response.setResponse(ByteString.copyFrom(s.getBytes()));
			response.setStatus(1);
			System.out.println("Assigned block " + available + " to " + filename);
		} catch (InvalidProtocolBufferException e) {
			System.out.println("Error assigning blocks: InvalidProtocolBufferException");
			e.printStackTrace();
			response.setResponse(ERROR_MSG);
			response.setStatus(-1);
			return response.build().toByteArray();
		}
		
		return response.build().toByteArray();
	}
		
	public byte[] list(byte[] inp ) throws RemoteException
	{
		NameNodeResponse.Builder response = NameNodeResponse.newBuilder();
		
		try{
			String list = "";
			for(FileInfo f : fileList) {
				list = list.concat(f.filename + " ");
			}
			response.setResponse(ByteString.copyFrom(list.getBytes()));
			response.setStatus(1);
		}catch(Exception e){
			System.err.println("Error at list "+ e.toString());
			e.printStackTrace();
			response.setResponse(ERROR_MSG);
			response.setStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	// Datanode <-> Namenode interaction methods
	/*
	 * On startup and every x seconds (configurable) the DN uses NNStub.heartBeat()
	 * This allows the NN to populate its in memory MD with files and blocks
	 * blockReport() is unimplemented since all the info is contained within the heartbeat
	 */
	
	
	/**
	 * Unused
	 */
	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		return null;
//		try
//		{
//		}
//		catch(Exception e)
//		{
//			System.err.println("Error at blockReport "+ e.toString());
//			e.printStackTrace();
//			response.setStatus(-1);
//		}
//		return response.build().toByteArray();
	}
	
	/**
	 * Receives a filename, a block, and the data node it is coming from
	 */
	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		DataNodeResponse.Builder response = DataNodeResponse.newBuilder();
		//Deserialize input
		try {
			HeartBeat hb = HeartBeat.parseFrom(inp);
			//id, ip, port
			String dnInfo[] = hb.getNodeinfo().split(";");
			DataNode d = new DataNode(dnInfo[1], Integer.parseInt(dnInfo[2]), dnInfo[0]);
			System.out.println("\nReceived heartbeat from " + dnInfo[1] + ":" + dnInfo[2]);
			//Make a temporary list to hold all blocks
			ArrayList<FileInfo> received = new ArrayList<>();
			for(DataNodeBlocks dnb : hb.getData().getDataList()) {
				//Base case
				if(received.isEmpty() == true) {
					FileInfo f = new FileInfo(dnb.getFilename());
					f.Chunks.add(dnb.getBlock(0).getBlocknum());
					d.blocks.add(dnb.getBlock(0).getBlocknum());
					received.add(f);
				}else {
					//Check if we already saw the file
					boolean seen = false;
					for(FileInfo f : received) {
						if(f.filename.equals(dnb.getFilename())) {
							f.Chunks.add(dnb.getBlock(0).getBlocknum());
							d.blocks.add(dnb.getBlock(0).getBlocknum());
							seen = true;
							break;
						}
					}
					//Discovered file, add it
					if(seen == false) {
						FileInfo f = new FileInfo(dnb.getFilename());
						f.Chunks.add(dnb.getBlock(0).getBlocknum());
						d.blocks.add(dnb.getBlock(0).getBlocknum());
						received.add(f);
					}
				}
			}
			
			//Done reading, update info
			if(fileList.isEmpty() == true) {
				for(FileInfo f : received) {
					fileList.add(f);
				}
			}else {
				Iterator<FileInfo> it = fileList.iterator();
				ArrayList<FileInfo> toBeAdded = new ArrayList<>();
				//Remove old data
				while(it.hasNext()) {
					FileInfo oldFile = it.next();
					for(FileInfo newFile : received) {
						if(newFile.filename.equals(oldFile.filename)) {
							it.remove();
							toBeAdded.add(newFile);
							break;
						}
					}
				}
				//Update
				for(FileInfo f : toBeAdded) fileList.add(f);
			}
			
			//Update Alive Data Nodes
			if(dataNodes.isEmpty() == true) {
				dataNodes.add(d);
				d.alive = true;
			}else {
				Iterator<DataNode> it = dataNodes.iterator();
				//Remove old node
				while(it.hasNext()) {
					DataNode oldNode = it.next();
					if(d.serverName.equals(oldNode.serverName)) {
						it.remove();
						break;
					}
				}
				//Update
				dataNodes.add(d);
				d.alive = true;
			}
			response.setStatus(1); //OK
			
			System.out.println("\nHeartBeat summary:");
			System.out.println("--------------------------");
			for(DataNode dn : dataNodes) {
				System.out.println(d.serverName + " : " + dn.ip + ":" + dn.port);
				System.out.println("\tBlocks:" + dn.blocks.get(0) + "-" + dn.blocks.get(dn.blocks.size()-1));
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			response.setStatus(-1);
			return response.build().toByteArray();
		}
		
		return response.build().toByteArray();
	}
	
	public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException
	{
		//Set up name node
		NameNode nn = null;
		BufferedReader br;
		try{
			br = new BufferedReader(new FileReader("src/nn_config.txt"));
		}catch(FileNotFoundException e) {
			System.out.println("Could not find nn_config.txt");
			e.printStackTrace();
			return;
		}
		String line = br.readLine(); //ignore first line
		line = br.readLine(); //ignore block size
		while( (line = br.readLine()) != null) { //TODO loop not needed
			String parsedLine[] = line.split(";");
			//Create new name node
			nn = new NameNode(parsedLine[1], Integer.parseInt(parsedLine[2]), parsedLine[0]);
			System.out.println("Created Name Node: \n\t" + parsedLine[0] + ": " + parsedLine[1] + " Port = " + Integer.parseInt(parsedLine[2]));
		}
		br.close();
		
		//TODO probably dont need this since the MD should be reconstructed through blockReports
		//TODO MD just persists filenames --> let client know that file is unavailable if DN goes down
		
		//Get data nodes
		try{
			br = new BufferedReader(new FileReader("src/dn_config.txt"));
		}catch(FileNotFoundException e) {
			System.out.println("Could not find dn_config.txt");
			e.printStackTrace();
			return;
		}
        line = br.readLine();
        while( (line = br.readLine()) != null) {
        	String parsedLine[] = line.split(";");
        	dataNodes.add(new DataNode(parsedLine[1], Integer.parseInt(parsedLine[2]), parsedLine[0]));
        }
        br.close();
		
		//Enable service
		System.setProperty("java.rmi.server.hostname" , nn.ip);
		System.setProperty("java.security.policy","src/permission.policy");
		if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
		
		//Set up server
		try {
			//Create name node stub
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(nn, 0);
			//Create registry on localhost
			LocateRegistry.createRegistry(nn.port);
			//Get registry reference
//			nn.serverRegistry = LocateRegistry.getRegistry(nn.ip, nn.port);
			nn.serverRegistry = LocateRegistry.getRegistry(nn.port);
			System.out.println(nn.serverRegistry);
			//bind vs rebind - bind throws an exception if the name in the registry 
			//is already bound by something else such as a datanode
			//this means that the namenode needs to be running and ready before starting the data node
			nn.serverRegistry.bind(nn.name, stub);
			
			//This message needs to be seen before the data node is started
			System.out.println(nn.name + " ready");
		}catch(Exception e) {
			System.out.println("Error setting up Name Node");
			e.printStackTrace();
			return;
		}
		
		//Timeout for dead DNs
		TimerTask timeoutQuery = new TimerTask() {
			@Override
			public void run() {
				Iterator<DataNode> it = dataNodes.iterator();
				while(it.hasNext()) {
					DataNode d = it.next();
					if(d.alive == false) {
						System.out.println(d.serverName + " at " + d.ip + ":" + d.port + " has gone down");
						it.remove();
					}
					d.alive = false; //This sets all nodes back to false so they can be queried in heartBeat()
				}
			}
		};
		Timer t = new Timer();
		t.scheduleAtFixedRate(timeoutQuery, timeout, timeout);
	}
}
