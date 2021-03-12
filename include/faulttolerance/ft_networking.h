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

#include <networklayer/connectionless.hh>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <kvcg_logging.h>
#include <kvcg_errors.h>

//Define address handle type
typedef int kvcg_addr_t;  // int is for socket fd

// Every server has an instance of net data
struct net_data_t {
    // address to send data out or receive on
    kvcg_addr_t addr;

    // This is socket specific
    struct sockaddr_in address;
};

// TBD: socket specific?
inline
int kvcg_close(kvcg_addr_t addr) {
  if (addr) {
    shutdown(addr, SHUT_RDWR);
    close(addr);
  }
  return 0;
}

// Return bytes read
// return 0 when remote closed
// return negative on error (or if we closed)
inline
int kvcg_read(kvcg_addr_t addr, void* buf, size_t count) {
  return read(addr, buf, count);
}

// Return bytes sent
// return negative on error
inline
int kvcg_send(kvcg_addr_t addr, const void* buf, size_t len, int flags) {
  return send(addr, buf, len, flags);
}

// Return negative on error, otherwise an address
inline
kvcg_addr_t kvcg_accept(net_data_t* net_data) {
  int addrlen = sizeof(net_data->address);
  LOG(DEBUG4) << "Waiting to accept on " << net_data->addr;
  return accept(net_data->addr, (struct sockaddr *)&net_data->address,
    (socklen_t*)&addrlen);
}

// Open an endpoint for listening on
inline
int kvcg_open_endpoint(net_data_t* net_data, int port) {
    int status = KVCG_ESUCCESS;
    int opt = 1;

    net_data->addr = socket(AF_INET, SOCK_STREAM, 0);
    if (net_data->addr == 0) {
        perror("socket failure");
        status = KVCG_EUNKNOWN;
        goto exit;
    }
    if (setsockopt(net_data->addr, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        status = KVCG_EUNKNOWN;
        goto exit;
    }

    net_data->address.sin_family = AF_INET;
    net_data->address.sin_addr.s_addr = INADDR_ANY;
    net_data->address.sin_port = htons(port);


    if (bind(net_data->addr, (struct sockaddr*)&net_data->address, sizeof(net_data->address)) < 0) {
        perror("bind error");
        status = KVCG_EUNKNOWN;
        goto exit;
    }

    if (listen(net_data->addr, 3) < 0) {
        perror("listen");
        status = KVCG_EUNKNOWN;
        goto exit;
    }

    LOG(DEBUG3) << "Listening on " << net_data->addr << "/" << port;

exit:
    LOG(DEBUG) << "Exit (" << status << "): " << kvcg_strerror(status);
    return status;   
}

// Connect to an open endpoint
inline
int kvcg_connect(net_data_t* net_data, std::string dst, int port) {
  int status = KVCG_ESUCCESS;
  struct hostent *he;
  bool connected = false;
  while (!connected) {
    net_data->addr = socket(AF_INET, SOCK_STREAM, 0);
    if (net_data->addr < 0) {
      perror("socket");
      status = KVCG_EUNKNOWN;
      goto exit;
    }

    net_data->address.sin_family = AF_INET;
    net_data->address.sin_port = htons(port);

    // Handle hostnames
    if ((he = gethostbyname(dst.c_str())) == NULL) {
        perror("gethostbyname");
        status = KVCG_EUNKNOWN;
        goto exit;
    }
    memcpy(&net_data->address.sin_addr, he->h_addr_list[0], he->h_length);

    LOG(DEBUG4) << "Attempting connection to " << net_data->addr << "/" << port;
    if(connect(net_data->addr, (struct sockaddr *)&net_data->address, sizeof(net_data->address)) < 0) {
      close(net_data->addr); // need to retry
      LOG(DEBUG4) << "  Connection failed, retrying";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      continue;
    }

    connected = true;
  }

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
kvcg_addr_t new_addr = kvcg_accept(&server_net_data);

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
