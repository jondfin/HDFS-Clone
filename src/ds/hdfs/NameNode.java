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

import ds.hdfs.hdfsProto.NameNodeData;
import ds.hdfs.hdfsProto.NameNodeResponse;

import com.google.protobuf.*;

public class NameNode implements INameNode{

	private static ArrayList<String> fileMD;
	
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
		int filehandle;
		boolean writemode;
		ArrayList<Integer> Chunks;
		public FileInfo(String name, int handle, boolean option)
		{
			filename = name;
			filehandle = handle;
			writemode = option;
			Chunks = new ArrayList<Integer>();
		}
	}
	
	/* Method to open a file given file name with read-write flag*/
	
	boolean findInFilelist(int fhandle)
	{
		return false;
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
	
	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		NameNodeResponse.Builder response = NameNodeResponse.newBuilder();
		
		//Check if file exists
		if(findInFilelist() == false) {
			System.out.println("File does no exist!");
			response.setStatus(-1);
			return response.build().toByteArray();
		}
		
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at getBlockLocations "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}		
		return response.build().toByteArray();
	}
	
	
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
		
		for(String nnd : md.getDataList()) {
			fileMD.add(nnd);
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
