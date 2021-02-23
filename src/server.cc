/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>

void Server::connHandle(int socket) {
  LOG(INFO) << "Handling connection";
  char buffer[1024] = {0};
  int r = read(socket, buffer, 1024);
  LOG(INFO) << "Read: " << buffer;
}

void Server::server_listen() {
  LOG(INFO) << "Waiting for Client requests...";
  while(true) {
    // FIXME: This is placeholder for network-layer
    int new_socket;
    int addrlen = sizeof(net_data.address);
    if ((new_socket = accept(net_data.server_fd, (struct sockaddr *)&net_data.address,
            (socklen_t*)&addrlen)) < 0) {
        perror("accept");
        exit(1);
    }
    // launch handle thread
    std::thread connhandle_thread(&Server::connHandle, this, new_socket);
    connhandle_thread.detach();
  }
}

int Server::log_put(int key, size_t valueSize, char* value) {
    // Send transaction to backups
    LOG(INFO) << "Logging PUT (" << key << "): " << value;
    BackupPacket pkt(key, valueSize, value);
    char* rawData = pkt.serialize();

    int dataSize = sizeof(key) + sizeof(valueSize) + valueSize;
    LOG(DEBUG4) << "raw data: " << rawData;
    LOG(DEBUG4) << "data size: " << dataSize;

    // TODO: Backup in parallel - dependent on network-layer
    // datagram support
    for (auto backup : backupServers) {
        LOG(DEBUG) << "Backing up to " << backup->getName();
        // send(backup->socket, rawData, dataSize);
    }

    return true;
}

#define PORT 8080
int Server::open_client_endpoint() {
    // TODO: This will be reworked by network-layer
    LOG(INFO) << "Opening Socket for Clients";
    int opt = 1;

    net_data.server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (net_data.server_fd == 0) {
        perror("socket failure");
        return 1;
    }
    if (setsockopt(net_data.server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        return 1;
    }

    net_data.address.sin_family = AF_INET;
    net_data.address.sin_addr.s_addr = INADDR_ANY;
    net_data.address.sin_port = htons(PORT);

    if (bind(net_data.server_fd, (struct sockaddr*)&net_data.address, sizeof(net_data.address)) < 0) {
        perror("bind error");
        return 1;
    }

    if (listen(net_data.server_fd, 3) < 0) {
        perror("listen");
       return 1;
    }

    return 0;
}

int Server::open_backup_endpoints() {
    // TODO: This will be reworked by network-layer
    LOG(INFO) << "Opening Backup Sockets for other Primaries";

    // TODO: Implement - open a socket to accept connection from
    //       all servers that this server is a backup for.

    return 0;
}

int Server::connect_backups() {
    // TODO: This will be reworked by network-layer
    LOG(INFO) << "Connecting to Backups";

    // TODO: Implement - have open connection to all servers backing
    //       us up, so that on PUT requests, we can send to backups.


    return 0;
}

int Server::initialize() {
    int status = 0;
    std::thread listen_thread;

    LOG(INFO) << "Initializing Server";

    if (status = parse_json_file(this))
        goto exit;


    // Log this server configuration
    LOG(INFO) << "Hostname: " << this->getName();
    LOG(INFO) << "Primary Keys:";
    for (auto keyRange : primaryKeys) {
        LOG(INFO) << "  [" << keyRange.first << ", " << keyRange.second << "]";
    }
    LOG(INFO) << "Backup Servers:";
    for (auto backup : backupServers) {
        LOG(INFO) << "  " << backup->getName();
    }
    LOG(INFO) << "Backing up primaries:";
    for (auto primary : primaryServers) {
        LOG(INFO) << "  " << primary->getName();
        for (auto keyRange : primary->getPrimaryKeys()) {
            LOG(DEBUG) << "    [" << keyRange.first << ", " << keyRange.second << "]";
        }
    }

    // Open connection for other servers to backup here
    if (status = open_backup_endpoints())
        goto exit;

    // Connect to this servers backups
    if (status = connect_backups())
        goto exit;

    // Open connection for clients
    if (status = open_client_endpoint())
        goto exit;

    // Start listening for clients
    listen_thread = std::thread(&Server::server_listen, this);
    listen_thread.join();

exit:
    return status;
}

bool Server::addKeyRange(std::pair<int, int> keyRange) {
  // TODO: Validate input
  primaryKeys.push_back(keyRange);
  return true;
}

bool Server::addPrimaryServer(Server* s) {
  // TODO: Validate input
  primaryServers.push_back(s);
  return true;
}


bool Server::addBackupServer(Server* s) {
  // TODO: Validate input
  backupServers.push_back(s);
  return true;
}


bool Server::isPrimary(int key) {
    for (auto el : primaryKeys) {
        if (key >= el.first && key <= el.second) {
            return true;
        }
    }
    return false;
}
