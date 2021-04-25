package keyValueStore.keyValue.replicaServers;

import keyValueStore.keyValue.KeyValue;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;


import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;

import keyValueStore.keyValue.util.*;

public class ReplicaKsServer {
    public static void readSeverConfigarartion(FileProcessor fp, ServerHandler serverHandler) throws IOException {
        while (true) {
            String line = fp.poll();
            if (line == null) {
                break;
            }
            String[] splitValue;
            splitValue = line.split(" ");
            serverHandler.serversIpaddr.put(splitValue[0], splitValue[1]);
            serverHandler.serversPortNo.put(splitValue[0], Integer.parseInt(splitValue[2]));
        }
    }
    public static boolean readLog(String fileName){
   boolean read=false;
    File check = new File(fileName);
        if(check.isFile()) {
        read=true;
    }
		else {
        //sets readable to false if the file does not exist.
            read=false;
    }
		return read;
    }
    public static void main(String[] args) throws IOException {

       // System.out.println(args.length);
        if(args.length != 4){
            System.out.println("Usage: ./server.sh <server name> <port> <server info file> <readRepair 0 or 1 >>\n");
            System.exit(0);
        }

        String severname = args[0];
        String portName = args[1];

        Boolean readRepairSetter=false;

        if(Integer.parseInt(args[3])== 1)
            readRepairSetter=true;
        //FileProcessor fileProcessor = new FileProcessor("/home/darshan/Documents/Distributive_System/Assignment4/commit/cs457-557-spring20-pa4-ddoddag1-sgowdra1/keyValueStore/serversConfig.txt");
        FileProcessor fileProcessor = new FileProcessor(args[2]);
        ServerHandler serverHandler = new ServerHandler(severname, Integer.parseInt(portName));

        readSeverConfigarartion(fileProcessor, serverHandler);

        //log file, path = /writeAheadLog/servername.log.
        String path = "writeAheadLog/" + serverHandler.getServerName() +".log";
       // FileProcessor readLog = new FileProcessor(path);

        try {
        //checks if the log file exists or not.. true -> reads log file, false -> file doesn't exist.
        if(readLog(path)) {
            FileProcessor fp = new FileProcessor(path);
            serverHandler.readLog(fp);
            fileProcessor.close();
            serverHandler.printstoreKeyValue();
        }}
        catch(Exception exception){
            System.out.println(exception);
        }

        WriteAheadlog wal = new WriteAheadlog("writeAheadLog/" + severname+".log");


        try {
            serverHandler.server = new ServerSocket(serverHandler.port);
            System.out.println("Relica server started");
//                System.out.println("Listening on " + InetAddress.getLocalHost().getHostAddress() +" " + + serverHandler.port);
        } catch (IOException i) {
            System.out.println(i);
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    KeyValue.KeyValueMessage.Builder keyValuemessage = KeyValue.KeyValueMessage.newBuilder();
                    //set the connection as from replica servers
                    keyValuemessage.setConnectionClient(0);
                    keyValuemessage.setServerName(serverHandler.getServerName());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException exception) {
                        exception.printStackTrace();
                    }
                    //always check for any available servers
                    for (String serverName : ServerHandler.serversIpaddr.keySet()) {
                        try {
                            Socket socket = new Socket(ServerHandler.serversIpaddr.get(serverName), ServerHandler.serversPortNo.get(serverName));
                            OutputStream out = socket.getOutputStream();
                            keyValuemessage.build().writeDelimitedTo(out);
                            serverHandler.addConnectedServers(serverName, true);
                            out.flush();
                            out.close();
                            socket.close();
                            // System.out.println("Servers connected is "+ServerHandler.serversIpaddr.get(serverName)+""+ ServerHandler.serversPortNo.get(serverName));
                        } catch (ConnectException e) {
                            // System.out.println(serverName + "is offline");
                            serverHandler.addConnectedServers(serverName, false);
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            serverHandler.addConnectedServers(serverName, false);
                            e.printStackTrace();
                        }
                    }
                }
            }
        }).start();


        Socket requestClient = null;

        while (true) {
            try {
                requestClient = ServerHandler.server.accept();
                InputStream in = requestClient.getInputStream();
                KeyValue.KeyValueMessage keyValueMessage = KeyValue.KeyValueMessage.parseDelimitedFrom(in);

                //Connection == 1  message from client.
                if (keyValueMessage.getConnectionClient() == 1) {
                    //send it to co-ordinator
                    System.out.println("Received message from Client...");
                    Thread coordinatorThread = new Thread(new Coordinator(requestClient, serverHandler, keyValueMessage,readRepairSetter));
                    coordinatorThread.start();
                }

                //   Connection == 0 message from replica servers.
                if (keyValueMessage.getConnectionClient() == 0) {
                    //message sent from co-ordinator to replica the data
                    // System.out.println("Received message from Server...");
                    String receiveServer = keyValueMessage.getServerName();
                    //System.out.println(receiveServer);

                    KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
                    OutputStream out = null;

                    if (keyValueMessage.hasPutKey()) {

                        // System.out.println("Key message has put");
                        KeyValue.Put put = keyValueMessage.getPutKey();
                        KeyValue.KeyValuePair keyStore = put.getKeyval();

                        String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
                        //log file
                        wal.writeToFile(writeAheadLog);
                        ServerHandler.storeKeyValue.put(keyStore.getKey(), keyStore);
                        //build a reply back to server
                        KeyValue.WriteResponse.Builder writeresponse = KeyValue.WriteResponse.newBuilder();
                        writeresponse.setKey(keyStore.getKey());
                        writeresponse.setId(put.getId());
                        writeresponse.setWriteReply(true);
                        System.out.println("Key value is stored in keystore\n" + writeAheadLog);

                        //reply back to Coordinator after message written to KeyStore.
                        out = requestClient.getOutputStream();
                        keyMessage.setWriteResponse(writeresponse.build());
                        keyMessage.build().writeDelimitedTo(out);
                        out.flush();
                        out.close();

                    }

                    if (keyValueMessage.hasGetKey()) {
                        System.out.println("Key message has get");
                        int key = keyValueMessage.getGetKey().getKey();
                        KeyValue.KeyValuePair keyStore = null;
                        KeyValue.ReadResponse.Builder readRespMsg = KeyValue.ReadResponse.newBuilder();
                        //check for the sent key
                        if (ServerHandler.storeKeyValue.containsKey(key)) {
                            System.out.println("keystore contaisn the key");
                            keyStore = ServerHandler.storeKeyValue.get(key);
                            readRespMsg.setKeyval(keyStore);
                            readRespMsg.setId(keyValueMessage.getGetKey().getId());
                            readRespMsg.setReadStatus(true);
                        } else {
                            KeyValue.KeyValuePair.Builder ks = KeyValue.KeyValuePair.newBuilder();
                            ks.setKey(key);
                            readRespMsg.setKeyval(ks);
                            readRespMsg.setId(keyValueMessage.getGetKey().getId());
                            readRespMsg.setReadStatus(false);
                            System.out.println("readFailed");
                            //read status is fasle
                        }
                        System.out.println("Read response sent...");
                        //    System.out.println("reply back to Coordinator.");
                        out = requestClient.getOutputStream();
                        keyMessage.setReadResponse(readRespMsg);
                        keyMessage.build().writeDelimitedTo(out);
                        out.flush();
                        out.close();
                    }

                    in.close();
                    requestClient.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
