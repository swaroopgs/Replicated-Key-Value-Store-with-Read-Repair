A severConfig file is used to store all local server.

Use the format Servername ip portnumber

server4 127.0.0.1 9093

How to compile and run
----------------------------------------------------------------------------------

1. To generate keyvalue class, use the following command
```
bash
export PATH=/home/yaoliu/src_code/local/bin:$PATH
protoc --java_out=./ keyValue.proto
```
2. To compile:
Navigate to project folder and then run the following command
```make```

3. To run application,
let be  servers >2 beacuse Consistency is set to 2 always: 

To run server use the command
```
./server.sh <servername><portnumber><serverConfig file path><readrepair>

$ ./server.sh server3 9092 /home/darshan/Documents/Distributive_System/Assignment4/commit/cs457-557-spring20-pa4-ddoddag1-sgowdra1/keyValueStore/serversConfig.txt 1

```
To run client use the command
```
./client.sh <cordinatorName> <serverConfig file path>```

$ ./client.sh server1 /home/darshan/Documents/Distributive_System/Assignment4/commit/cs457-557-spring20-pa4-ddoddag1-sgowdra1/keyValueStore/serversConfig.txt 

going to set coordinator server as server1
Enter key value like put-5-25 or get-5
put-25-oooo
Enter key value like put-5-25 or get-5
read write thread

Received response from co-ordinator..!!
25 true

get-25
Enter key value like put-5-25 or get-5
read write thread

Received response from co-ordinator..!!
true
key ::25 oooo

``
`` 
