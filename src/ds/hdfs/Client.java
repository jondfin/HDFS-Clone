package ds.hdfs;
import java.net.InetAddress;
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

import ds.hdfs.hdfsProto.Block;
import ds.hdfs.hdfsProto.ClientQuery;
import ds.hdfs.hdfsProto.DataNodeResponse;
//import ds.hdfs.INameNode;
import ds.hdfs.hdfsProto.NameNodeResponse;

public class Client
{
	private static long blockSize = 64;
	
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    
    public Client()
    {
    	try {
			//Read the nn_config to get info
			BufferedReader br = new BufferedReader(new FileReader("src/nn_config.txt"));
			String line = br.readLine();
			line = br.readLine(); //Get blocksize
			blockSize = Long.parseLong(line);
			//Get name, ip, and port
			line = br.readLine();
			br.close();
			String parsedLine[] = line.split(";");
			//Create new name node
			NNStub = GetNNStub(parsedLine[0], parsedLine[1], Integer.parseInt(parsedLine[2]));
			System.out.println("Retrieved Name Node stub");
			
		}catch(Exception e) {
			System.out.println("Error starting client");
			e.printStackTrace();
			return;
		}
    }

    public IDataNode GetDNStub(String Name, String IP, int Port)
    {
        while(true)
        {
//        	System.out.println("Looking for " + Name + " at " + IP + ":" + Port);
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
            	e.printStackTrace();
                continue;
            }
        }
    }

    public INameNode GetNNStub(String Name, String IP, int Port)
    {
    	System.out.println("Name: " + Name);
    	//TODO temp
    	try {
			IP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
//			e1.printStackTrace();
		}
    	System.out.println("IP: " + IP);
    	System.out.println("Port: " + Port);
        while(true)
        {
        	System.out.println("Looking for " + Name);
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("Found NN");
                return stub;
            }catch(Exception e){
//            	e.printStackTrace();
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
    	File f = new File(Filename);
    	if(!f.exists()) {
    		System.out.println("File does not exist locally");
    		return;
    	}else if(f.length() == 0) {
    		System.out.println("Cannot put an empty file into HDFS");
    		return;
    	}
    	try {
    		//Ask Name Node to put file
        	ClientQuery.Builder cq = ClientQuery.newBuilder();
        	cq.setFilename(f.getName());
			NameNodeResponse blockLocations = NameNodeResponse.parseFrom(NNStub.getBlockLocations(cq.build().toByteArray()));
			if(blockLocations.getStatus() == 1) {
				System.out.println("OK from server...Reading bytes from file");
				//Keep track of blocks written to file
				ClientQuery.Builder open = ClientQuery.newBuilder();
				open.setFilename(f.getName());
				NameNodeResponse openResp = NameNodeResponse.parseFrom(NNStub.openFile(open.build().toByteArray()));
				//Check if there is space in HDFS
				if((f.length() / blockSize) > openResp.getStatus()) {
					System.out.println("Unable to put " + Filename + " into HDFS");
					return;
				}
				//Start reading bytes from file
				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
				int bytesRead = 0;
				byte buffer[] = new byte[(int)blockSize];
				while( (bytesRead = bis.read(buffer)) > 0) {
					cq = ClientQuery.newBuilder();
					cq.setFilename(f.getName());
					//Get block from Name Node
					NameNodeResponse nameNodeBlockResponse = NameNodeResponse.parseFrom(NNStub.assignBlock(cq.build().toByteArray()));
					if(nameNodeBlockResponse.getStatus() == -1) {
						System.out.println("Error getting block from Name Node...Aborting");
						System.out.println("Couldn't put file into HDFS");
						bis.close();
						return;
					}
					//Returns as blocknumber;ip;port;name
					String blockInfo[] = nameNodeBlockResponse.getResponse().toStringUtf8().split(";");
					int blockNum = Integer.parseInt(blockInfo[0]);
					
					//Get the data node to send block to
					DNStub = GetDNStub(blockInfo[3], blockInfo[1], Integer.parseInt(blockInfo[2]));
					
					//Write block to Data Node
					Block.Builder b = Block.newBuilder();
					b.setBlocknum(blockNum);
					b.setData(ByteString.copyFrom(buffer));
					DataNodeResponse response = DataNodeResponse.parseFrom(DNStub.writeBlock(b.build().toByteArray()));
					if(response.getStatus() == -1) {
						System.out.println("Error writing blocks to data node...Aborting");
						System.out.println("Couldn't put file into HDFS");
						bis.close();
						return;
					}
					//Make sure buffer is emptied out
					Arrays.fill(buffer, (byte)0);				
				}
				bis.close();
				//Have the name node commit meta data to disk
				ClientQuery.Builder close = ClientQuery.newBuilder();
				close.setFilename(f.getName());
				NameNodeResponse closeResp = NameNodeResponse.parseFrom(close.build().toByteArray());
				if(closeResp.getStatus() == -1) {
					System.out.println("Error: Could not put " + Filename + " into HDFS");
					return;
				}
			}else {
				System.out.println("File already exists in HDFS!");
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

    /**
     * Retrieve file from HDFS to put into local
     * @param Name of file to be brought from HDFS
     */
    public void GetFile(String Filename)
    {
	    //Create query
	    ClientQuery.Builder cq = ClientQuery.newBuilder();
	    cq.setFilename(Filename);
	    
	    //Ask Name node for file
	    try {
	    	//This will return locations of the blocks if the file exists within HDFS
	    	NameNodeResponse blockLocations = NameNodeResponse.parseFrom(NNStub.getBlockLocations(cq.build().toByteArray()));
	    	if(blockLocations.getStatus() == 0) {
	    		System.out.println("File does not exist in HDFS!");
	    		return;
	    	}
	    	//Block locations are returned in the form
	    	//ip;port;id;block1,block2,...,blockN
	    	String parsedBL[] = blockLocations.getResponse().toStringUtf8().split(";");
	    	
	    	//Get Data Node to connect to
	    	DNStub = GetDNStub(parsedBL[2], parsedBL[0], Integer.parseInt(parsedBL[1]));
	    	
	    	//Create file locally
	    	File f = new File(Filename);
	    	if(f.exists() == true) {
	    		System.out.println("File already exists locally!");
	    		return;
	    	}
	    	f.createNewFile();
	    	FileOutputStream fos = new FileOutputStream(f, true);
	    	//Assumption: all blocks are in order and the data will be reassembled in order
	    	for(String blockNum : parsedBL[3].split(",")) {
	    		//Create a new block and serialize it
	    		Block.Builder block = Block.newBuilder();
	    		block.setBlocknum(Integer.parseInt(blockNum));
	    		DataNodeResponse data = DataNodeResponse.parseFrom(DNStub.readBlock(block.build().toByteArray()));
	    		if(data.getStatus() == -1) {
	    			System.out.println("Error: Could not retrieve block from Data Node");
	    			fos.close();
	    			System.out.println(f.delete());
	    			return;
	    		}
	    		fos.write(data.getResponse().toStringUtf8().getBytes());
	    	}
	    	fos.close();
	    }catch(RemoteException e) {
	    	System.out.println("Error retrieving file from HDFS: RemoteException");
	    	e.printStackTrace();
	    	return;
	    } catch (InvalidProtocolBufferException e) {
	    	System.out.println("Error retrieving file from HDFS: InvalidProtocolBufferException");
			e.printStackTrace();
			return;
		} catch (IOException e) {
			System.out.println("Error creating file from HDFS: IOException");
			e.printStackTrace();
			return;
		}
	    
    }

    /**
     * Get list of files from HDFS
     */
    public void List()
    {
    	try {
			NameNodeResponse files = NameNodeResponse.parseFrom(NNStub.list(null));
			String resp = files.getResponse().toStringUtf8().trim();
			if(!resp.isEmpty())	System.out.println(files.getResponse().toStringUtf8().trim());
		} catch (RemoteException e) {
			System.out.println("Error getting file list: RemoteException");
			e.printStackTrace();
			return;
		} catch (InvalidProtocolBufferException e) {
			System.out.println("Error getting file list: InvalidProtocolBufferException");
			e.printStackTrace();
			return;
		}
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
