
# time stamp format in this program
TIME_FORMAT_STRING = '%H:%M:%S'

# static introducer addr
INTRODUCER_HOST = 'fa18-cs425-g73-01.cs.illinois.edu'


# made for introducer

ALL_HOSTS = []

for i in range(9):
    index = str(i+1)
    ALL_HOSTS.append('fa18-cs425-g73-0'+index + '.cs.illinois.edu')

ALL_HOSTS.append('fa18-cs425-g73-10.cs.illinois.edu')

ALL_HOSTS = sorted(ALL_HOSTS)




NEXT_TWO = {
    'fa18-cs425-g73-01.cs.illinois.edu': [
        'fa18-cs425-g73-02.cs.illinois.edu',
        'fa18-cs425-g73-03.cs.illinois.edu',
    ],
    'fa18-cs425-g73-02.cs.illinois.edu': [
        'fa18-cs425-g73-03.cs.illinois.edu',
        'fa18-cs425-g73-04.cs.illinois.edu',
    ],
    'fa18-cs425-g73-03.cs.illinois.edu': [
        'fa18-cs425-g73-04.cs.illinois.edu',
        'fa18-cs425-g73-05.cs.illinois.edu',
    ],
    'fa18-cs425-g73-04.cs.illinois.edu': [
        'fa18-cs425-g73-05.cs.illinois.edu',
        'fa18-cs425-g73-06.cs.illinois.edu',
    ],
    'fa18-cs425-g73-05.cs.illinois.edu': [
        'fa18-cs425-g73-06.cs.illinois.edu',
        'fa18-cs425-g73-07.cs.illinois.edu',
    ],
    'fa18-cs425-g73-06.cs.illinois.edu': [
        'fa18-cs425-g73-07.cs.illinois.edu',
        'fa18-cs425-g73-08.cs.illinois.edu',
    ],
    'fa18-cs425-g73-07.cs.illinois.edu': [
        'fa18-cs425-g73-08.cs.illinois.edu',
        'fa18-cs425-g73-09.cs.illinois.edu',
    ],
    'fa18-cs425-g73-08.cs.illinois.edu': [
        'fa18-cs425-g73-09.cs.illinois.edu',
        'fa18-cs425-g73-10.cs.illinois.edu',
    ],
    'fa18-cs425-g73-09.cs.illinois.edu': [
        'fa18-cs425-g73-10.cs.illinois.edu',
        'fa18-cs425-g73-08.cs.illinois.edu',
    ],
    'fa18-cs425-g73-10.cs.illinois.edu': [
        'fa18-cs425-g73-02.cs.illinois.edu',
        'fa18-cs425-g73-09.cs.illinois.edu',

    ]
}


# default socket port
DEFAULT_PORT_FD = 52333
DEFAULT_PORT_SDFS = 53222
DEFAULT_PORT_CRANE = 53666

# topology of 10 VMs
# {source: [dest_1, dest_2, ...]}
CONNECTIONS = {
    'fa18-cs425-g73-01.cs.illinois.edu': [
        'fa18-cs425-g73-09.cs.illinois.edu',
        'fa18-cs425-g73-10.cs.illinois.edu',
        'fa18-cs425-g73-02.cs.illinois.edu',
        'fa18-cs425-g73-03.cs.illinois.edu',
    ],
    'fa18-cs425-g73-02.cs.illinois.edu': [
        'fa18-cs425-g73-03.cs.illinois.edu',
        'fa18-cs425-g73-04.cs.illinois.edu',
        'fa18-cs425-g73-01.cs.illinois.edu',
        'fa18-cs425-g73-10.cs.illinois.edu',
    ],
    'fa18-cs425-g73-03.cs.illinois.edu': [
        'fa18-cs425-g73-04.cs.illinois.edu',
        'fa18-cs425-g73-05.cs.illinois.edu',
        'fa18-cs425-g73-01.cs.illinois.edu',
        'fa18-cs425-g73-02.cs.illinois.edu',
    ],
    'fa18-cs425-g73-04.cs.illinois.edu': [
        'fa18-cs425-g73-05.cs.illinois.edu',
        'fa18-cs425-g73-06.cs.illinois.edu',
        'fa18-cs425-g73-02.cs.illinois.edu',
        'fa18-cs425-g73-03.cs.illinois.edu',
    ],
    'fa18-cs425-g73-05.cs.illinois.edu': [
        'fa18-cs425-g73-06.cs.illinois.edu',
        'fa18-cs425-g73-07.cs.illinois.edu',
        'fa18-cs425-g73-03.cs.illinois.edu',
        'fa18-cs425-g73-04.cs.illinois.edu',
    ],
    'fa18-cs425-g73-06.cs.illinois.edu': [
        'fa18-cs425-g73-07.cs.illinois.edu',
        'fa18-cs425-g73-08.cs.illinois.edu',
        'fa18-cs425-g73-04.cs.illinois.edu',
        'fa18-cs425-g73-05.cs.illinois.edu',
    ],
    'fa18-cs425-g73-07.cs.illinois.edu': [
        'fa18-cs425-g73-08.cs.illinois.edu',
        'fa18-cs425-g73-09.cs.illinois.edu',
        'fa18-cs425-g73-05.cs.illinois.edu',
        'fa18-cs425-g73-06.cs.illinois.edu',
    ],
    'fa18-cs425-g73-08.cs.illinois.edu': [
        'fa18-cs425-g73-09.cs.illinois.edu',
        'fa18-cs425-g73-10.cs.illinois.edu',
        'fa18-cs425-g73-06.cs.illinois.edu',
        'fa18-cs425-g73-07.cs.illinois.edu',
    ],
    'fa18-cs425-g73-09.cs.illinois.edu': [
        'fa18-cs425-g73-10.cs.illinois.edu',
        'fa18-cs425-g73-01.cs.illinois.edu',
        'fa18-cs425-g73-07.cs.illinois.edu',
        'fa18-cs425-g73-08.cs.illinois.edu',
    ],
    'fa18-cs425-g73-10.cs.illinois.edu': [
        'fa18-cs425-g73-01.cs.illinois.edu',
        'fa18-cs425-g73-02.cs.illinois.edu',
        'fa18-cs425-g73-08.cs.illinois.edu',
        'fa18-cs425-g73-09.cs.illinois.edu',
    ]
}


