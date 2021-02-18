# fault-tolerance

Fault Tolerance portion of KVCG System

## Table of Contents

- [Build](#build)
- [Test](#testing)
- [Features/API](#features)
- [Team](#team)

## Build <a name="build"></a>
### Prerequisites
- No prerequisites yet...

## Testing <a name="testing"></a>
The unittest suite provided in test/ can be built with the included Makefile:
```
cd test/; make
```
It can run as either a server or a client:
```
./unittest_fault_tolerance     <-- Start as server
./unittest_fault_tolerance -C  <-- Start as client
```

## Features <a name="features"></a>
The API is defined as follows:
### int init_server()
Initialize server
### int init_client()
Initialize client

## Team <a name="team"></a>
- [Cody D'Ambrosio](https://github.com/cjd218)
- [Olivia Grimes](https://github.com/oag221)
- [Jacob Oakman](https://github.com/jco222)

Contact us: [email](mailto:cjd218@lehigh.edu,oag221@lehigh.edu,jco222@lehigh.edu?subject=[GitHub]%20KVCG_Fault_Tolerance)
