package keyValueStore.keyValue.replicaServers;

import java.util.HashMap;


public class ReadRepairKeyStore {

    private int id = 0;
    private int key = 0;
    private String value = null;
    private long timestamp = 0;
    private HashMap<String,Boolean> servers = new HashMap<String,Boolean>();
    public HashMap<String,Long> serversTimestamp = new HashMap<String,Long>();
    private Boolean readRepairStatus = false;
    private Boolean readStatus = false;

    public ReadRepairKeyStore(int idIn, int keyIn, String valueIn, long timeIn) {
        id = idIn;
        key = keyIn;
        value = valueIn;
        timestamp = timeIn;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int keyIn) {
        key = keyIn;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String valueIn) {
        value = valueIn;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestampIn) {
        timestamp = timestampIn;
    }

    public int getId() {
        return id;
    }

    public void setId(int idIn) {
        id = idIn;
    }

    public HashMap<String,Boolean> getServers() {
        return servers;
    }

    public void addServers(String serverName, Boolean b) {
        servers.put(serverName, b);
    }

    public void updateServers() {
        for(String serverNames: servers.keySet()) {
            servers.replace(serverNames, false);
        }
    }

    public Boolean getReadRepairStatus() {
        return readRepairStatus;
    }

    public void setReadRepairStatus(Boolean statusIn) {
        readRepairStatus = statusIn;
    }

    public Boolean getReadStatus() {
        return readStatus;
    }

    public void setReadStatus(Boolean readStatusIn) {
        readStatus = readStatusIn;
    }

    public Boolean checkConsistency(int consistencyIn) {
        int consistencyValue = 0;
        boolean consistency=false;
        for(String name: serversTimestamp.keySet()) {
            if(serversTimestamp.get(name) == timestamp) {
                consistencyValue++;
                if(consistencyValue == consistencyIn) {
                    consistency= true;
                }
            }
        }
        return consistency;
    }

}
