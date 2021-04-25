package keyValueStore.client;
import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;
import keyValueStore.keyValue.KeyValue;
class Client {

    static Socket socket = null;
    static String coordinator = null;
    static String ipAddresscoordinator = null;
    static int portNumbercoordinator = 0;
    static BufferedReader bufferedReader = null;

    public static void main(String [] args) {
         coordinator = args[0];
         fileReader(args[1]);
//        coordinator = "server1";
//        fileReader("keyValueStore/serversConfig.txt");
        Scanner scan = new Scanner(System.in);
        System.out.println("going to set coordinator server as "+ coordinator);
        Thread startReadAndWriteThread = new Thread(new Runnable() {

            @Override
            public void run() {
                while(true) {
                    try {
                        InputStream in = socket.getInputStream();
                        KeyValue.KeyValueMessage responseMessage = KeyValue.KeyValueMessage.parseDelimitedFrom(in);

                        //System.out.println("read write thread");
                        if(responseMessage != null) {

                            System.out.println("\nReceived response from co-ordinator..!!");

                            if(responseMessage.hasWriteResponse()) {

                                KeyValue.WriteResponse writeResponse = responseMessage.getWriteResponse();
                                System.out.println(writeResponse.getKey() + " " + writeResponse.getWriteReply());

                            }

                            if(responseMessage.hasReadResponse()) {

                                KeyValue.ReadResponse readResponse = responseMessage.getReadResponse();
                                KeyValue.KeyValuePair keyStore = readResponse.getKeyval();

                                if(readResponse.getReadStatus()) {
                                    System.out.println(readResponse.getReadStatus());
                                    System.out.println("key ::"+keyStore.getKey() + " " + keyStore.getValue());
                                }
                                if(!readResponse.getReadStatus() ){
                                    System.out.println("Key " + keyStore.getKey() + " not present in  store" );
                                }

                            }

                        }
                    }
                        catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
        });

        while(true) {
            System.out.println("Enter key value like put-5-25 or get-5");
            String keyValue = scan.next();

            if(keyValue == null){
                System.out.println("Enter key value like put-5-25");
                break;
            }

            String[] splitkeyValue;
            splitkeyValue = keyValue.split("-");
           // KeyValue.KeyValueMessage.Builder keyvalueBuilder = null;
            KeyValue.KeyValueMessage.Builder keyvalueBuilder = KeyValue.KeyValueMessage.newBuilder();

            try{
                int key =Integer.parseInt(splitkeyValue[1]);
                String value ;
                int clevel=2;
                try {

                    if(splitkeyValue[0].equalsIgnoreCase("get")) {

                        KeyValue.Get.Builder getMethod = KeyValue.Get.newBuilder();
                        getMethod.setKey(key);
                        getMethod.setConsistency(clevel);

                        keyvalueBuilder.setGetKey(getMethod.build());


                    }
                    if(splitkeyValue[0].equalsIgnoreCase("put")) {

                        //  key = Integer.parseInt(splitkeyValue[1]);
                        value = splitkeyValue[2];
                        KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
                        keyStore.setKey(key);
                        keyStore.setValue(value);

                        KeyValue.Put.Builder putMethod = KeyValue.Put.newBuilder();
                        putMethod.setKeyval(keyStore.build());
                        putMethod.setConsistency(clevel);
                        keyvalueBuilder.setPutKey(putMethod.build());
                    }
                } catch(NumberFormatException i) {
                    System.out.println("Enter key value like put-5-25 or get-5");
                    continue;
                } catch(Exception e) {
                    System.out.println("Enter key value like put-5-25 or get-5");

                    continue;
                }
            }
            catch(NumberFormatException exception) {
                System.out.println("Wrong key value pair must be int...");
            }
            try {
                if(socket == null) {
                    //co-ordinator
                    socket = new Socket(ipAddresscoordinator, portNumbercoordinator);
                    keyvalueBuilder.setConnectionClient(1);
                    startReadAndWriteThread.start();
                }

                OutputStream out = socket.getOutputStream();
                keyvalueBuilder.build().writeDelimitedTo(out);
                out.flush();

            } catch(ConnectException e) {
                System.out.println("Server offline....");
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Server offline....");
                e.printStackTrace();
            }
        }
    }





    public static void fileReader(String fileName) {
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                System.out.println("Config File does not exit! please provide Absolute path");
                System.exit(0);
            }
            bufferedReader = new BufferedReader(new FileReader(file));
            String line = "";
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    String[] nameIp = line.split(" ");
                    String serverName = nameIp[0];
                    String serverIp = nameIp[1];
                    int serverPort = Integer.parseInt(nameIp[2]);
                    if(coordinator.equalsIgnoreCase(serverName)) {
                        ipAddresscoordinator = serverIp;
                        portNumbercoordinator = serverPort;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.print("File not found");
            e.printStackTrace();
            System.exit(0);
        }
    }

}
