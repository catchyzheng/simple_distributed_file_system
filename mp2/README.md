Put server.py on each machine, and then run:

```
python3 server.py
```

when the screen appears :

```
Enter your command(lsm, lsid, join or leave):
```

'lsm' can list the member list on this machine. 

```
Time[1538953126.633977]: [('fa18-cs425-g73-01.cs.illinois.edu', 12, 1538953125.9010024), ('fa18-cs425-g73-02.cs.illinois.edu', 11, 1538953125.9332037), ('fa18-cs425-g73-03.cs.illinois.edu', 14, 1538953125.900105), ('fa18-cs425-g73-04.cs.illinois.edu', 19, 1538953125.9029346), ('fa18-cs425-g73-05.cs.illinois.edu', 15, 1538953125.9019616), ('fa18-cs425-g73-06.cs.illinois.edu', 8, 1538953125.9038942)]
```

'lsid' can list the domain name of this machine. 

```
Time[1538953410.0909734]: fa18-cs425-g73-01.cs.illinois.edu
```

'join' can add a machine. After joined, the log will be:

```
Time[1538953121.123178]: fa18-cs425-g73-05.cs.illinois.edu is joining.
```

'leave' can let machine leave. After leaved, the log will be:

```
Time[1538953121.123178]: fa18-cs425-g73-05.cs.illinois.edu volunterally left,
```


When a machine fails and be detected, the log will be:

```
Time[1538953263.5525844]: fa18-cs425-g73-04.cs.illinois.edu has gone offline
```




