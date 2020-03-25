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

	private static ArrayList<FileInfo> fileList;
	
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
	
	public void printFilelist()
	{
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
	 * Returns 1 and block locations if the file exists, 0 if the file does not exist
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
			return null;
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
	 * Input is a byte array that contains the filename and filesize
	 * Returns 1 if blocks assigned, -1 otherwise
	 */
	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at AssignBlock "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		
		return response.build().toByteArray();
	}
		
	
	public byte[] list(byte[] inp ) throws RemoteException
	{
		try
		{
		}catch(Exception e)
		{
			System.err.println("Error at list "+ e.toString());
			e.printStackTrace();
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
		String line = br.readLine();
		while( (line = br.readLine()) != null) {
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
		}catch(Exception e) {
			System.out.println("Name Node meta-data file does not exist");
			File f = new File("src/NNMD.txt");
			f.createNewFile(); //create new file if not found
			md = NameNodeData.parseFrom(new FileInputStream(new File("src/NNMD.txt")));
			System.out.println("Created meta-data file");
		}
		
		//Bring file metadata into memory from storage file
		//Lines are stored as such: <filename>:[block, block, ..., block]
		//ex. a.txt:1,5,13
		for(ByteString nnd : md.getDataList()) { //TODO change to String maybe?
			String parsedLine[] = nnd.toString().split(":"); //split between filename and blocks
			String blocks[] = parsedLine[1].split(","); //parse out the block numbers
			
			//Create fileinfo
			FileInfo f = new FileInfo(parsedLine[0]);
			for(String blockNum : blocks) {
				f.Chunks.add(Integer.parseInt(blockNum));
			}
			fileList.add(f);
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
