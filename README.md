# fault-tolerance

Fault Tolerance portion of KVCG System

## Table of Contents

- [Build](#build)
- [Test](#testing)
- [Features/API](#features)
- [Team](#team)

## Build <a name="build"></a>
### Prerequisites
- boost

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

## Features <a name="features"></a>
Be sure to include header:
```
#include "fault_tolerance.h"
```

All API calls return an integer status. If the call was successful, the return value will be 0. Otherwise, a non-zero value will be returned.

A sample configuration file is provided in test/:
```
{
  "servers": [
    {
      "name": "hdwtpriv37",
      "minKey": 0,
      "maxKey": 100,
      "backups": ["hdwtpriv38"]
    },
    {
      "name": "hdwtpriv38",
      "minKey": 101,
      "maxKey": 200,
      "backups": ["hdwtpriv37"]
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
Server* server = new Server();
server->initialize();
```

### Initialize Client
Initialize the running host as a client. This includes
- Parsing configuration file
```
Client* client = new Client();
client->initialize();
```

### Log Transaction
Log a PUT transaction by sending the data to all backup servers. This may be done with a single key/value pair, or a batch of pairs.
```
// Store value 20 at key 5
server->log_put<int, int>(5, 20);

// Store the key/value pairs 4/40, 6/60, 7/70
std::vector<int> keys {4, 6, 7};
std::vector<int> values {40, 60, 70};
server->log_put<int, int>(keys, values);
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
