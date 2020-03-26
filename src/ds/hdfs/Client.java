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
	protected static long blockSize = 64;
	
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    public Client()
    {
		try {
			//Read the nn_config to get info
			BufferedReader br = new BufferedReader(new FileReader("src/nn_config.txt"));
			String line = br.readLine();
			line = br.readLine();
			blockSize = Long.parseLong(line);
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
    	System.out.println("Going to put file " + Filename);
    	
    	try {
    		//Ask Name Node to put file
        	ClientQuery.Builder cq = ClientQuery.newBuilder();
        	cq.setFilename(Filename);
			NameNodeResponse blockLocations = NameNodeResponse.parseFrom(NNStub.getBlockLocations(cq.build().toByteArray()));
			if(blockLocations.getStatus() == -1) {
				//Start reading bytes from file
				File f = new File(Filename);
				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
				int bytesRead;
				byte buffer[] = new byte[(int)blockSize];
				while( (bytesRead = bis.read(buffer)) > 0) {
					cq.clear();
					cq.setFilename(Filename);
					//Get block from Name Node
					NameNodeResponse blockNum = NameNodeResponse.parseFrom(NNStub.assignBlock(cq.build().toByteArray()));
					//Write block to Data Node
					Block b = Block.newBuilder();
					b.setBlocknum = blockNum.getResponse();
					b.setData = ByteString.copyFrom(buffer);
					DataNodeResponse response= DNStub.writeBlock(b.build().toByteArray());
					if(response.getStatus() == -1) {
						System.out.println("Error writing blocks to data node...Aborting");
						System.out.println("Couldn't put file into HDFS");
						bis.close();
						return;
					}
				}
				bis.close();
			}else {
				System.out.println("File already exists!");
				return;
			}
		} catch (RemoteException e) {
			System.out.println("Error putting file into HDFS: RemoteException");
			e.printStackTrace();
			return;
		} catch (InvalidProtocolBufferException e) {
			System.out.println("Error putting file into HDFS: InvalidProtocolBufferException");
			e.printStackTrace();
			return;
		} catch (FileNotFoundException e) {
			System.out.println("Error putting file into HDFS: FileNotFoundException");
			e.printStackTrace();
			return;
		} catch (IOException e) {
			System.out.println("Error putting file into HDFS: IOException");
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
