# fault-tolerance [![CI Actions Status](https://github.com/CSE-498-011-SP21/fault-tolerance/workflows/C//C++%20CI/badge.svg)](https://github.com/CSE-498-011-SP21/fault-tolerance/actions)

Fault Tolerance portion of KVCG System

## Table of Contents

- [Build](#build)
- [Test](#testing)
- [Features/API](#features)
- [Team](#team)

## Build <a name="build"></a>
### Prerequisites
- boost-devel  (ubuntu: libboost-all-dev)
#### Inherited from network-layer
- libfabric-devel (ubuntu: libfabric-dev)
- tbb-devel (ubuntu: libtbb-dev)

Be sure to update submodules as well:
```
git submodule update --init --recursive
```

## Testing <a name="testing"></a>
The unittest suite provided in test/ can be built with the included Makefile:
```
cd test/; make
```
It can run as either a server or a client:
```
Usage: unittest_fault_tolerance [OPTIONS]
  -c [CONFIG] : Config JSON file (Default: ./kvcg.json)
  -C          : Run as client, defaults to running as server
  -v          : Increase verbosity
  -h          : Print this help text

```

### Testing with Docker
In order to build several containers and network them together we first must build the image.
```
$> docker build -t fault-tolerance .
```
Then we must set up a docker network for the containers to be a part of. A bridge network seems to work well for this.
```
$> docker network create -d bridge ft_network
```
After that, individual containers can be built using the following commands where ~/path/to/fault-tolerance represents your local path to where the fault-tolerance codebase is stored.
```
$> docker container create -it --hostname node1 --name node1 --network ft_network -v ~/path/to/fault-tolerance:/fault-tolerance fault-tolerance
$> docker container create -it --hostname node2 --name node2 --network ft_network -v ~/path/to/fault-tolerance:/fault-tolerance fault-tolerance
$> docker container create -it --hostname node3 --name node3 --network ft_network -v ~/path/to/fault-tolerance:/fault-tolerance fault-tolerance
```
Then these containers can be run in seperate terminal windows with
```
$> docker container start -i node1
```

## Features <a name="features"></a>
Be sure to include header:
```
#include "faulttolerance/fault_tolerance.h"
```

All API calls return an integer status. If the call was successful, the return value will be 0. Otherwise, a non-zero value will be returned.

A sample configuration file is provided in test/. The libfabric provider may be either 'verbs' or 'sockets', and will default to 'sockets' if not specified. A specific server address can optionally be provided in the case where the desired NIC address does not match the server name. If a server is only a backup and not a primary for any keys, but needs a specific address, it can be specified under servers with no minKey or maxKey.
```
{
  "provider": "verbs",
  "servers": [
    {
      "name": "hdwtpriv37",
      "address": "192.168.1.1",      <-- ensure verbs NIC address is used
      "minKey": 0,
      "maxKey": 100,
      "backups": ["hdwtpriv38"]
    },
    {
      "name": "hdwtpriv38",          <-- no address defined, will resolve from hostname
      "minKey": 101,
      "maxKey": 200,
      "backups": ["hdwtpriv39"]
    },
    {
      "name": "hdwtpriv39",          <-- backup only, but need to use a specific address
      "address": "192.168.1.3"
    }
  ]
}
```

### Initialize Server
Initialize the running host as a server. This includes
- Parsing configuration file
- Connecting to backup servers for the local server
- Connecting to other primary servers who the local server is backing up
- Start listening for incoming client requests
```
namespace ft = cse498::faulttolerance;

ft::Server* server = new ft::Server();
server->initialize("kvcg.json");
```

### Initialize Client
Initialize the running host as a client. This includes
- Parsing configuration file
```
ft::Client* client = new ft::Client();
client->initialize("kvcg.json");
```

### Log Transaction
Log a PUT transaction by sending the data to all backup servers. This may be done with a single key/value pair, or a batch of pairs.
```
// Store value 20 at key 5
server->log_put(5, 20);

// Store the key/value pairs 4/'word1', 6/'word2', 7/'word3'
std::vector<unsigned long long> keys {4, 6, 7};
std::vector<data_t*> values;
for (int i=0; i<3; i++) {
  data_t* value = new data_t();
  value->data = "word" + std::to_string(i+1).c_str();
  value->size = 5;
  values.push_back(value);
}
server->log_put(keys, values);
```

### Shutdown Server
Safely close server.
```
server->shutdownServer();
```

## Team <a name="team"></a>
- [Cody D'Ambrosio](https://github.com/cjd218)
- [Olivia Grimes](https://github.com/oag221)
- [Jacob Oakman](https://github.com/jco222)

Contact us: [email](mailto:cjd218@lehigh.edu,oag221@lehigh.edu,jco222@lehigh.edu?subject=[GitHub]%20KVCG_Fault_Tolerance)
