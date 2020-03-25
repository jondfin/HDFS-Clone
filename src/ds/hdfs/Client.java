package ds.hdfs;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import ds.hdfs.hdfsProto.ClientQuery;
import ds.hdfs.hdfsProto.DataNodeResponse;
//import ds.hdfs.INameNode;
import ds.hdfs.hdfsProto.NameNodeResponse;

public class Client
{
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    public Client()
    {
		try {
			//Read the nn_config to get info
			BufferedReader br = new BufferedReader(new FileReader("src/nn_config.txt"));
			String line = br.readLine();
			while( (line = br.readLine()) != null) {
				String parsedLine[] = line.split(";");
				//Create new name node
				NNStub = GetNNStub(parsedLine[0], parsedLine[1], Integer.parseInt(parsedLine[2]));
				System.out.println("Retrieved Name Node stub");
			}
			br.close();
			
			//Read the dn_config to get info
			br = new BufferedReader(new FileReader("src/dn_config.txt"));
			line = br.readLine();
			while( (line = br.readLine()) != null) {
				String parsedLine[] = line.split(";");
				DNStub = GetDNStub(parsedLine[0], parsedLine[1], Integer.parseInt(parsedLine[2]));
				System.out.println("Retrieved Data Node stub");
			}
			br.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
    }

    public IDataNode GetDNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                continue;
            }
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
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

    /**
     * Write to HDFS from local
     * @param Name of file to be sent to HDFS
     */
    public void PutFile(String Filename) //Put File
    {
	        System.out.println("Going to put file" + Filename);
	        BufferedInputStream bis;
	        int filesize = -1;
	        try{
	        	//Read bytes from file
	            bis = new BufferedInputStream(new FileInputStream(new File(Filename)));
	        }catch(Exception e){
	            System.out.println("File not found !!!");
	            return;
	        }
	        try {
	        	bis.close();
	        }catch(Exception e) {
	        	System.out.println("Error closing inputstream");
	        }
	        
	        //Create protobuf message
	    	ClientQuery.Builder cq = ClientQuery.newBuilder();
	    	cq.setFilename(Filename);
            byte inp[] = cq.build().toByteArray();
            
            //Query NameNode
            byte queryResponse[];
            NameNodeResponse nameNodeResponse;
            try {
				queryResponse = NNStub.getBlockLocations(inp); //will not return block locations if the file does not exist
	            nameNodeResponse = NameNodeResponse.parseFrom(queryResponse);
	            if(nameNodeResponse.getStatus() == 0) { //File does not exist, can put 
	            	//Have the Name Node assign blocks
	            	ClientQuery.Builder cq2 = ClientQuery.newBuilder();
	            	cq2.setFilename(Filename);
	            	cq2.setFilesize(filesize);
	            	byte blockLocations[] = NNStub.assignBlock(cq2.build().toByteArray());
	            	//Send bytes to Data Node to write
	            	DataNodeResponse dataNodeResponse = DataNodeResponse.parseFrom(DNStub.writeBlock(blockLocations));
	            	if(dataNodeResponse.getStatus() == 1) {
	            		System.out.println("Successfully put " + Filename + " into HDFS");
	            		return;
	            	}else {
	            		System.out.println("Error: Could not put file into HDFS");
	            	}
	            }else { //File already exists, cannot put
	            	System.out.println("Error: File already exists!");
	            	return;
	            }
			} catch (RemoteException e) {
				System.out.println("Error getting block locations: RemoteException");
				System.out.println("Could not put file into HDFS");
				e.printStackTrace();
				return;
			} catch (InvalidProtocolBufferException e) {
				System.out.println("Error getting block locations: InvalidProtocolBufferException");
				System.out.println("Could not put file into HDFS");
				e.printStackTrace();
				return;
			}
	    	
    }

    public void GetFile(String Filename)
    {
	    System.out.println("Going to get file" + Filename);
	    BufferedInputStream bis;
	    try{
//	        bis = new BufferedInputStream(new FileInputStream(File));
	    }catch(Exception e){
	        System.out.println("File not found !!!");
	        return;
	    }
    }

    public void List()
    {
    	System.out.println("Getting file list");
    }

    public static void main(String[] args) throws RemoteException, UnknownHostException
    {
        // To read config file and Connect to NameNode
        //Intitalize the Client
        Client Me = new Client();
        System.out.println("Welcome to HDFS!!");
        Scanner Scan = new Scanner(System.in);
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.nextLine();
            String[] Split_Commands = Command.split(" ");

            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS"); System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))  // put Filename
            {
                //Put file into HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.PutFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Get file from HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.GetFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Get list of files in HDFS
                Me.List();
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}
