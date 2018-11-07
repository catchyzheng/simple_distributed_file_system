



Introduction(added after due):

For this MP, we are basically using Java, and therefore, in order to implement the grep command, we are using the grep4j library to satisfy the grep requirements. We first build a profile for each log file on ten VMs, and set the path to the remote log file and add credentials with username and password for them. Then we sent grep command to each VM and then return the grep results matching the pattern. 

As for unit test, we made a design that on each VM we use some simple log file to check whether it can grep properly to the other VMs. If all VM work well, then we can start formal grepping. 



Instruction: (written before due):

All the codes and tests are written together in mp1.java file, so just run the lines below is ok:

Compile mp1.java with related jar package:

`````
javac -cp grep4j.jar mp1/src/main/java/mp1.java
`````

run mp1.java:

```
java -cp grep4j.jar mp1/src/main/java mp1
```







