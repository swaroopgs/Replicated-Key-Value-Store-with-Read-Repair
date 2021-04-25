package keyValueStore.keyValue.replicaServers;

import keyValueStore.keyValue.KeyValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

import keyValueStore.keyValue.KeyValue;

import java.net.ConnectException;
public class Coordinator extends Thread {
    private Socket clinetsocketRequest;
    private ServerHandler serverHandler =null;
    KeyValue.KeyValueMessage keyValueMessage;
    private HashMap<Integer, Integer> consistencyHashMap = new HashMap<Integer, Integer>();
    private HashMap<Integer, Integer> writeResponseHashMap = new HashMap<Integer, Integer>();
    private HashMap<Integer, Integer> readResponseHashMap = new HashMap<Integer,Integer>();
    private HashMap<Integer, Integer> replies = new HashMap<Integer,Integer>();
    private HashMap<Integer, ReadRepairKeyStore> readRepairHashMap=new HashMap<Integer,ReadRepairKeyStore>();
    private boolean setReadRepairStatus =false;
    public Coordinator(Socket request, ServerHandler serverHandlerIn, KeyValue.KeyValueMessage keyValueMsg,boolean setReadRepairStatusIn) {
        clinetsocketRequest = request;
        serverHandler = serverHandlerIn;
        keyValueMessage = keyValueMsg;
        setReadRepairStatusSetter(setReadRepairStatusIn);

    }

    public void setReadRepairStatusSetter(boolean setReadRepairStatusIn){
        setReadRepairStatus=setReadRepairStatusIn;
    }
    public void run() {
        System.out.println("cordinator responing");
        if (keyValueMessage != null) {
            //System.out.println("keyValueMessage is not null");
            handleClientRequest(keyValueMessage);

        }
        while (true) {
            try {
                InputStream in = clinetsocketRequest.getInputStream();
                KeyValue.KeyValueMessage incomingMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
                if (incomingMsg != null) {
                    handleClientRequest(incomingMsg);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleClientRequest(KeyValue.KeyValueMessage incomingMsg) {

        KeyValue.KeyValueMessage.Builder keyValueBuilder = null;

        //received put message from client and has put key
        if (incomingMsg.hasPutKey()) {
            System.out.println("Put as co-ordinator");
            Date date = new Date();
            KeyValue.Put putRequest = incomingMsg.getPutKey();
            int consistency = putRequest.getConsistency();

            KeyValue.Put.Builder putServer = KeyValue.Put.newBuilder();
            KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();

            keyStore.setKey(putRequest.getKeyval().getKey());
            keyStore.setValue(putRequest.getKeyval().getValue());
            keyStore.setTime(date.getTime());

            putServer.setKeyval(keyStore.build());
            putServer.setConsistency(consistency);
            putServer.setId(putRequest.getId());

            consistencyHashMap.put(putServer.getId(), consistency);
            writeResponseHashMap.put(putServer.getId(), 0);
            keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
            //connection set to 0 to inform co-ordinator about replica
            keyValueBuilder.setConnectionClient(0);
            keyValueBuilder.setPutKey(putServer.build());
            sendToReplicaServers(keyValueBuilder);
        }
        if(incomingMsg.hasGetKey()) {
            System.out.println("get from co-ordinator");
            KeyValue.Get getMessage = incomingMsg.getGetKey();
            int consistency = getMessage.getConsistency();

            KeyValue.Get.Builder getServer = KeyValue.Get.newBuilder();

            getServer.setKey(getMessage.getKey());
            getServer.setConsistency(getMessage.getConsistency());
            getServer.setId(getMessage.getId());

            consistencyHashMap.put(getMessage.getId(), consistency);
            readResponseHashMap.put(getMessage.getId(), 0);
            replies.put(getMessage.getId(), 0);

            keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
            keyValueBuilder.setConnectionClient(0);
            keyValueBuilder.setGetKey(getServer.build());
            sendToReplicaServers(keyValueBuilder);
            }

        }





    private void sendToReplicaServers(KeyValue.KeyValueMessage.Builder keyMsgIn) {
        System.out.println("sending message to ReplicaServers");
        for (String serverName : ServerHandler.serversIpaddr.keySet()) {
            try {
                keyMsgIn.setServerName(serverHandler.getServerName());
                System.out.println(ServerHandler.serversIpaddr.get(serverName));
                Socket socket = new Socket(ServerHandler.serversIpaddr.get(serverName), ServerHandler.serversPortNo.get(serverName));
                OutputStream out = socket.getOutputStream();
                keyMsgIn.build().writeDelimitedTo(out);
                serverHandler.addConnectedServers(serverName, true);
                out.flush();
                //Thread to listen response from the requested server
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            String servernameresopnse = serverName;
                            InputStream inputStream = socket.getInputStream();
                            KeyValue.KeyValueMessage responseMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(inputStream);
                            System.out.println("waiting for server response");
                            handleServerResponse(servernameresopnse, responseMsg);
                            inputStream.close();
                            socket.close();
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }).start();

            } catch (ConnectException e) {
                System.out.println(serverName + "not reachable and is offline");
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * used to handlerServerresponse
     * @param serverNameIn
     * @param responseMsg
     * @throws IOException
     */
    private synchronized void handleServerResponse(String serverNameIn, KeyValue.KeyValueMessage responseMsg) throws IOException {
        System.out.println("Checking for sever responses");
        if(responseMsg.hasWriteResponse()) {
            KeyValue.WriteResponse writeResponse = responseMsg.getWriteResponse();
            int id = writeResponse.getId();
            if(writeResponse.getWriteReply()) {
                int numnerreply = writeResponseHashMap.get(id);
                writeResponseHashMap.replace(id,numnerreply+1);
            }
            //IF total no. of response received is equal to consistency level, return to client
            if(consistencyHashMap.get(id) == writeResponseHashMap.get(id)) {
                System.out.println("Sending write response to client: key= " + writeResponse.getKey() + " " + writeResponse.getWriteReply());
                consistencyHashMap.replace(id, -1);
                //send response to client
                KeyValue.KeyValueMessage.Builder responseClient = KeyValue.KeyValueMessage.newBuilder();
                responseClient.setWriteResponse(writeResponse);
                try {
                    OutputStream out = clinetsocketRequest.getOutputStream();
                    responseClient.build().writeDelimitedTo(out);
                    out.flush();

                } catch(IOException i) {
                    System.out.println("Client is offline reachable...");
                    //i.printStackTrace();
                }
            }
        }

        if(setReadRepairStatus == true) {
            if (responseMsg.hasReadResponse()) {
                KeyValue.ReadResponse readResponse = responseMsg.getReadResponse();

                int id = readResponse.getId();
                int numnerreply = replies.get(id);
                replies.replace(id, numnerreply + 1);


                if (readResponse.getReadStatus()) {
                    // int id=readResponse.getId();
                    int numberofreply = readResponseHashMap.get(id);
                    readResponseHashMap.replace(id, numberofreply + 1);
                    int key = readResponse.getKeyval().getKey();
                    String value = readResponse.getKeyval().getValue();
                    long time = readResponse.getKeyval().getTime();

                    //first response received....
                    if (!readRepairHashMap.containsKey(id)) {
                        ReadRepairKeyStore r = new ReadRepairKeyStore(id, key, value, time);
                        r.addServers(serverNameIn, true);

                        readRepairHashMap.put(id, r);
                    }

                    readRepairHashMap.get(id).setReadStatus(true);
                    readRepairHashMap.get(id).serversTimestamp.put(serverNameIn, time);
                    //Checks if the received message has a timestamp greater than the one in readRepairMap for the id; if true replaces with the latest timestamp;
                    if (time > readRepairHashMap.get(id).getTimestamp()) {
                        readRepairHashMap.get(id).setId(id);
                        readRepairHashMap.get(id).setKey(key);
                        readRepairHashMap.get(id).setValue(value);
                        readRepairHashMap.get(id).setTimestamp(time);
                        readRepairHashMap.get(id).updateServers();
                        readRepairHashMap.get(id).setReadRepairStatus(true);
                        readRepairHashMap.get(id).addServers(serverNameIn, true);
                    }
                    //if received message has a timestamp lesser than the one in readRepairMap, then the corresponding server has to be sent the updated key-value pair
                    if (time < readRepairHashMap.get(id).getTimestamp()) {
                        //System.out.println("Read Repair should be performed on  " + serverName);
                        readRepairHashMap.get(id).addServers(serverNameIn, false);
                        readRepairHashMap.get(id).setReadRepairStatus(true);
                    }

//                int cVal = readResponseMap.get(id);
//                readResponseMap.replace(id, cVal+1);

                    //IF total no. of response received is equal to consistency level ==2, return to client
                    if (readResponseHashMap.get(id) >= 2) {
                        consistencyHashMap.replace(id, -1);
                        KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
                        KeyValue.ReadResponse.Builder rr = KeyValue.ReadResponse.newBuilder();
                        KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();

                        keyStore.setKey(readRepairHashMap.get(id).getKey());
                        keyStore.setValue(readRepairHashMap.get(id).getValue());
                        keyStore.setTime(readRepairHashMap.get(id).getTimestamp());
                        rr.setKeyval(keyStore.build());
                        rr.setId(readRepairHashMap.get(id).getId());
                        rr.setReadStatus(readRepairHashMap.get(id).getReadStatus());

                        keyMessage.setReadResponse(rr.build());
                        try {
                            OutputStream out = clinetsocketRequest.getOutputStream();
                            keyMessage.build().writeDelimitedTo(out);
                            out.flush();
                        } catch (IOException i) {
                            System.out.println("Client is offfline ...");

                        }
                    }


                }
                //return value null for the readStatus, means it does not have value and read repair has to be performed.
                else {
                    int key = readResponse.getKeyval().getKey();

                    if (!readRepairHashMap.containsKey(id)) {
                        ReadRepairKeyStore r = new ReadRepairKeyStore(id, key, null, 0);
                        r.addServers(serverNameIn, false);
                        readRepairHashMap.put(id, r);
                    }

                    System.out.println("Read Repair should be performed on(empty response) " + serverNameIn);
                    readRepairHashMap.get(id).addServers(serverNameIn, false);

                }

                if (replies.get(id) == serverHandler.getConnectedServers()) {
                    readRepairProcessoring(serverNameIn, id);
                }
                if(readRepairHashMap.get(id).getReadStatus() == false){
                    KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
                    KeyValue.ReadResponse.Builder rr = KeyValue.ReadResponse.newBuilder();
                    KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();

                    keyStore.setKey(readRepairHashMap.get(id).getKey());
                    rr.setKeyval(keyStore.build());
                    rr.setId(readRepairHashMap.get(id).getId());
                    rr.setReadStatus(readRepairHashMap.get(id).getReadStatus());

                    keyMessage.setReadResponse(rr.build());
                    try {
                        OutputStream out = clinetsocketRequest.getOutputStream();
                        keyMessage.build().writeDelimitedTo(out);
                        out.flush();
                    } catch(IOException i) {
                        System.out.println("Client not reachable...");
                        //i.printStackTrace();
                    }
                }

            }
        }

        if(setReadRepairStatus == false) {

        //    return value null for the readStatus, means it does not have value and read repair has to be performed.
            KeyValue.ReadResponse readResponse = responseMsg.getReadResponse();
            int id = readResponse.getId();
            int numnerreply = replies.get(id);
            replies.replace(id, numnerreply+1);
            if(readResponse.getReadStatus()) {

                int numberofreply=readResponseHashMap.get(id);
                readResponseHashMap.replace(id, numberofreply+1);
            }
            //IF total no. of response received is equal to consistency level ==2, return to client
            if( readResponseHashMap.get(id)>=2) {
                System.out.println("Sending read response to client: key= " + readResponse.getKeyval().getValue());
                //send response to client
                KeyValue.KeyValueMessage.Builder responseClient = KeyValue.KeyValueMessage.newBuilder();
                responseClient.setReadResponse(readResponse);

                try {
                    OutputStream out = clinetsocketRequest.getOutputStream();
                    responseClient.build().writeDelimitedTo(out);
                    out.flush();

                } catch(IOException i) {
                    System.out.println("Client is offlinereachable...");
                    //i.printStackTrace();
                }
            }
        }

        }

    private void readRepairProcessoring(String serverName, int id) {

        System.out.println("status " + readRepairHashMap.get(id).getReadRepairStatus());
        //check if readRepair has to be done or not
        if(readRepairHashMap.get(id).getReadStatus() == true) {
            KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
            KeyValue.ReadRepair.Builder readRepairMessage = KeyValue.ReadRepair.newBuilder();
            KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
            keyStore.setTime(readRepairHashMap.get(id).getTimestamp());

            keyStore.setValue(readRepairHashMap.get(id).getValue());
            keyStore.setKey(readRepairHashMap.get(id).getKey());

            readRepairMessage.setKeyval(keyStore.build());
            readRepairMessage.setId(id);

            keyMessage.setReadRepair(readRepairMessage.build());

            //list of server names that needs to be updated
            HashMap<String,Boolean> serversList = readRepairHashMap.get(id).getServers();

            for(String name : serversList.keySet()) {
                if(serversList.get(name) == false) {
                    try {

                        System.out.println("readRepair message is Sending to " + name + "  Key:  " + readRepairHashMap.get(id).getKey());
                        Socket socket = new Socket(ServerHandler.serversIpaddr.get(name), ServerHandler.serversPortNo.get(name));
                        OutputStream outputStream = socket.getOutputStream();
                        keyMessage.build().writeDelimitedTo(outputStream);
                        outputStream.flush();
                        outputStream.close();
                        socket.close();

                    } catch(ConnectException e) {
                        System.out.println("Server offline");
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


}



