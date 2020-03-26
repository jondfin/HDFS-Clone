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
	private static ArrayList<FileInfo> fileList;
	//Keep track of what blocks are being used
	private static BitSet bitset = new BitSet(Integer.MAX_VALUE); //might be too big 

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
		for(FileInfo file : fileList) {
			if(file.filename.equals(filename)) return file;
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
			response.setResponse(null);
    		response.setStatus(-1);
    		return response.build().toByteArray();
		}
		String filename = query.getFilename();
		
		//Check if file exists
		FileInfo f = findInFilelist(filename);
		if(f != null) {
			System.out.println("File does not exist!");
			response.setResponse(null);
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
	 * Returns an available block number
	 */
	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		NameNodeResponse.Builder response = NameNodeResponse.newBuilder();
		
		//Deserialize message
		try {
			ClientQuery query = ClientQuery.parseFrom(inp);
			int available = bitset.nextSetBit(0);
			if(available == -1) {
				System.out.println("Error assigning blocks: No more blocks left!");
				response.setStatus(-1);
				return null;
			}
			//Block is available, map it and send it to client
			String filename = query.getFilename();
			for(FileInfo f : fileList) {
				if(f.filename.equals(filename)) {
					f.Chunks.add(Integer.valueOf(available));
					break;
				}
			}
			response.setResponse(ByteString.copyFrom((String.valueOf(available).getBytes())));
			response.setStatus(1);
		} catch (InvalidProtocolBufferException e) {
			System.out.println("Error assigning blocks: InvalidProtocolBufferException");
			e.printStackTrace();
			response.setStatus(-1);
			return null;
		}
		
		return response.build().toByteArray();
		
		
//		ClientQuery query;
//		try{
//			query = ClientQuery.parseFrom(inp);
//			String filename = query.getFilename();
//			long filesize = query.getFilesize();
//			int numberOfBlocks = (int)(filesize / blockSize); //risking overflow here
//			int remainder = (int)(filesize % blockSize); //in case file is not a multiple of blockSize
//			numberOfBlocks += remainder;
//			
//			//Look for contiguous region of blocks
//			for(int i = 0; i < bitset.length(); i++) {
//				if(bitset.get(i, i+numberOfBlocks).isEmpty() == true) {
//					bitset.set(i, i+numberOfBlocks);
//					response.setResponse(null);
//					response.setStatus(1);
//					break;
//				}
//			}
//			
//		}catch(Exception e) {
//			System.err.println("Error at AssignBlock "+ e.toString());
//			e.printStackTrace();
//			response.setResponse(null);
//			response.setStatus(-1);
//		}
		
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
			response.setResponse(null);
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
	
	public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException
	{
		//Set up name node
		NameNode nn = null;
		BufferedReader br = new BufferedReader(new FileReader("src/nn_config.txt"));
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
			fileList = new ArrayList<>();
			System.out.println("Created meta-data file");
		}
		
		//Enable service
		System.setProperty("java.rmi.server.hostname" , "NameNode");
		System.setProperty("java.security.policy","src/permission.policy");
		if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
		
		//Set up server
		try {
			//Create name node stub
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(nn, 0);
			//Create registry
			LocateRegistry.createRegistry(nn.port);
			//Get registry reference
			nn.serverRegistry = LocateRegistry.getRegistry(nn.port);
			nn.serverRegistry.bind(nn.name, stub);
			
			System.out.println(nn.name + " ready");
		}catch(Exception e) {
			System.out.println("Error receiving from client");
			e.printStackTrace();
		}
		
	}
}
