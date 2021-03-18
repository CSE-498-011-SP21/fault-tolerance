/**
 *
 * Placeholder for network-layer API
 *
 * Example of how it is used by fault-tolerance
 * component at bottom.
 *
 */

#ifndef FT_NETWORKING_H
#define FT_NETWORKING_H

#include <networklayer/connection.hh>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <kvcg_logging.h>
#include <kvcg_errors.h>

// Every server has an instance of net data
struct net_data_t {
    // address to send data out or receive on
    cse498::Connection* conn;
};

// TBD: socket specific?
inline
int kvcg_close(cse498::Connection* addr) {
  return 0;
}

// Return bytes read
// return 0 when remote closed
// return negative on error (or if we closed)
inline
int kvcg_read(cse498::Connection* conn, void* buf, size_t count) {
  conn->wait_recv((char*)buf, count);
  return 1;
}

// Return bytes sent
// return negative on error
inline
int kvcg_send(cse498::Connection* conn, const void* buf, size_t len, int flags) {
  conn->wait_send((char*)buf, len);
  return 1;
}

// Return negative on error, otherwise an address
inline
cse498::Connection* kvcg_accept(net_data_t* net_data) {
  cse498::Connection* conn = new cse498::Connection();

  // Wait for initial connection
  char *buf = new char[128];
  conn->wait_recv(buf, 128);

  return conn;
}

// Open an endpoint for listening on
inline
int kvcg_open_endpoint(net_data_t* net_data, int port) {
    return KVCG_ESUCCESS;
}

// Connect to an open endpoint
inline
int kvcg_connect(net_data_t* net_data, std::string dst, int port) {
  int status = KVCG_ESUCCESS;

  std::string hello = "hello\0";

  LOG(DEBUG4) << "Attempting connection to " << dst;
  net_data->conn = new cse498::Connection(dst.c_str());

  // Initial send
  net_data->conn->wait_send(hello.c_str(), hello.length()+1);

exit:
  LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
  return status;
}



#if 0
/********************************************************************/
/***************** Example Usage (minor pseudocode) *****************/
/********************************************************************/

// Server opens connection
net_data_t server_net_data;
int status = kvcg_open_endpoint(&server_net_data, PORT);

// Server waits to accept connection
cse498::Connection new_addr = kvcg_accept(&server_net_data);

// Client connects to server
net_data_t client_net_data;
std::string serverName = "host1";
int status = kvcg_connect(&client_net_data, serverName, PORT);

// now can send back and forth.
// client sends to server
std::string msg = "hello";
kvcg_send(client_net_data.addr, msg.c_str(), msg.size(), 0);

// server receives message
char rcvbuf[64];
kvcg_read(server_net_data.addr, rcvbuf, 64);
std::cout << "Received: " << rcvbuf << std::endl;

#endif // end of example 


#endif // FT_NETWORKING_H
