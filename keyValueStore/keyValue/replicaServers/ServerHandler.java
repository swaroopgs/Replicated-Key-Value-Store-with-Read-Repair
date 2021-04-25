package keyValueStore.keyValue.replicaServers;

import java.io.*;
import java.net.ServerSocket;
import java.util.HashMap;
import keyValueStore.keyValue.KeyValue;
import keyValueStore.keyValue.util.*;

public class ServerHandler {

    public File file = null;
    public BufferedReader bufferedReader = null;

    private String serverName = null;
    static ServerSocket server;
    static int port = 0;

    //maintains status of the replica servers value, true == connected and false ==not connected.
    private HashMap<String,Boolean> connectedServers = new HashMap<String,Boolean>();

    static HashMap<String,String> serversIpaddr = new HashMap<String,String>();
    static HashMap<String,Integer> serversPortNo = new HashMap<String,Integer>();
    //In memory data structure to store key value pairs.
    static HashMap<Integer,KeyValue.KeyValuePair> storeKeyValue = new HashMap<Integer,KeyValue.KeyValuePair>();

    public ServerHandler(String name, int portNumber) {
        setServerName(name);
        port = portNumber;
    }


    public String getServerName() {
        return serverName;
    }

    public void setServerName(String nameIn) {
        serverName = nameIn;
    }



    public synchronized void addConnectedServers(String serverNameIn, Boolean statusIn) {

        if(connectedServers.containsKey(serverNameIn)) {
            connectedServers.replace(serverNameIn, statusIn);
        }
        else {
            connectedServers.put(serverNameIn, statusIn);
        }
    }
    public int getConnectedServers(){
        return connectedServers.size();
    }

    public void printstoreKeyValue() {
        for(int key: storeKeyValue.keySet()) {
            System.out.println(storeKeyValue.get(key).getKey() + "   " + storeKeyValue.get(key).getValue() + "   " + storeKeyValue.get(key).getTime());
        }
    }

    public void readLog(FileProcessor fp) {
        try{
        while(true) {
            String line = fp.poll();
            if(line == null) {
                break;
            }
            String [] splitValue;
            splitValue = line.split(" ");
            int key = Integer.parseInt(splitValue[0]);

            KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
            keyStore.setKey(key);
            keyStore.setValue(splitValue[1]);
            keyStore.setTime(Long.parseLong(splitValue[2]));

            if(storeKeyValue.containsKey(key)) {
                storeKeyValue.replace(key, keyStore.build());
            }
            else {
                storeKeyValue.put(key, keyStore.build());
            }
        }}
        catch (Exception e){
            System.out.println(e);
        }
    }


}
