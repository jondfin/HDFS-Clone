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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import ds.hdfs.hdfsProto.ClientQuery;
import ds.hdfs.hdfsProto.NameNodeData;
import ds.hdfs.hdfsProto.NameNodeResponse;

import com.google.protobuf.*;

public class NameNode implements INameNode{

	//Holds the list of files and their meta-data
	private static ArrayList<FileInfo> fileList = new ArrayList<>();
	//Keep track of what blocks are being used
	private static BitSet bitset = new BitSet(Integer.MAX_VALUE); //might be too big 
	//Keep track of the data nodes
	private static ArrayList<DataNode> dataNodes = new ArrayList<>();
	
	private static final ByteString ERROR_MSG = ByteString.copyFrom("NULL".getBytes());
	
	protected static long blockSize = 64; //Measured in bytes. Read in from nn_config. Default 64 Bytes
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
		public DataNode(String addr,int p,String sname)
		{
			ip = addr;
			port = p;
			serverName = sname;
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
			filename = name;
//			filehandle = handle;
//			writemode = option;
			Chunks = new ArrayList<Integer>();
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
		try
		{
		}
		catch (Exception e) 
		{
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		return response.toByteArray();
	}
	
	public byte[] closeFile(byte[] inp ) throws RemoteException
	{
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
			response.setResponse(ERROR_MSG); //not necessarily an error
			response.setStatus(0); //OK if the client wants to PUT file
			return response.build().toByteArray();
		}
		
		//File exists, can return block locations
		String blockLocations = null;
		for(Integer i : f.Chunks) {
			blockLocations = blockLocations.concat(i.toString());
		}
		response.setResponse(ByteString.copyFrom(blockLocations.getBytes()));
		response.setStatus(1); //OK
		
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
			String list = null;
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
		
	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at blockReport "+ e.toString());
			e.printStackTrace();
			response.addStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		return response.build().toByteArray();
	}
	
	public void printMsg(String msg)
	{
		System.out.println(msg);		
	}
	
	/**
	 * Write to NNMD.txt
	 * File structure is:
	 * file1:block1,block2,...
	 * ...
	 * fileN:block1,block2,...
	 */
	private void writeMD() {
		try {
			FileOutputStream fos = new FileOutputStream(new File("NNMD.txt"));
			for(FileInfo f : fileList) {
				String line = f.filename + ":";
				int count = 0;
				for(Integer block : f.Chunks) {
					line = line.concat(String.valueOf(block));
					if(count < f.Chunks.size() - 1) line = line.concat(",");
					count++;
				}
			}
			fos.close();
		}catch(FileNotFoundException e) {
			System.out.println("Error: Could not find meta data file!");
		} catch (IOException e) {
			System.out.println("Error: IOException");
		}
		return;
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
		line = br.readLine(); //get block size
		blockSize = Long.parseLong(line);
		while( (line = br.readLine()) != null) { //TODO loop not needed
			String parsedLine[] = line.split(";");
			//Create new name node
			nn = new NameNode(parsedLine[1], Integer.parseInt(parsedLine[2]), parsedLine[0]);
			System.out.println("Created Name Node: \n\t" + parsedLine[0] + ": " + parsedLine[1] + " Port = " + Integer.parseInt(parsedLine[2]));
		}
		br.close();
		
		//Initialize meta-data
		NameNodeData md;
		try{
			//Read from file
			md = NameNodeData.parseFrom(new FileInputStream(new File("src/NNMD.txt")));
			//Bring file metadata into memory from storage file
			//Lines are stored as such: <filename>:[block, block, ..., block]
			//ex. a.txt:1,5,13
			for(String nnd : md.getDataList()) {
				String parsedLine[] = nnd.toString().split(":"); //split between filename and blocks
				String blocks[] = parsedLine[1].split(","); //parse out the block numbers
				
				//Create fileinfo
				FileInfo f = new FileInfo(parsedLine[0]);
				for(String blockNum : blocks) {
					f.Chunks.add(Integer.parseInt(blockNum));
					bitset.set(Integer.parseInt(blockNum));
				}
				fileList.add(f);
			}
		}catch(Exception e) {
			System.out.println("Name Node meta-data file does not exist");
			File f = new File("src/NNMD.txt");
			f.createNewFile(); //create new file if not found
			md = NameNodeData.parseFrom(new FileInputStream(new File("src/NNMD.txt")));
			System.out.println("Created meta-data file");
		}
		
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
		
	}
}
