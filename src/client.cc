/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include "kvcg_config.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <string.h>
#include <sstream>
#include <boost/asio/ip/host_name.hpp>
#include "kvcg_errors.h"

int Client::initialize() {
    int status = 0;
    LOG(INFO) << "Initializing Client";

    KVCGConfig kvcg_config;
    if (status = kvcg_config.parse_json_file(CFG_FILE))
        LOG(INFO) << "Failed to parse config file";
        goto exit;
    
    this->serverList = kvcg_config.getServerList();

    if (status = this->connect_servers()) {
        LOG(INFO) << "Failed to connect to servers";
        goto exit;
    }

exit:
    return status;
}



#define PORT 8080
int Client::connect_servers() {
// TODO: This will be reworked by network-layer
    LOG(INFO) << "Connecting to Servers";

    int sock;
    struct hostent *he;

    for (auto server: this->serverList) {
        LOG(DEBUG) << "  Connecting to " << server->getName();
        bool connected = false;
        while (!connected) {
            struct sockaddr_in addr;
            if ((sock = socket(AF_INET, SOCK_STREAM, 0)) <0) {
                perror("socket");
                return KVCG_EUNKNOWN;
            }

            addr.sin_family = AF_INET;
            addr.sin_port = htons(PORT+1); // still ugly
            if ((he = gethostbyname(server->getName().c_str())) == NULL ) {
                perror("gethostbyname");
                return KVCG_EUNKNOWN;
            }
            memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

            if(connect(sock, (struct sockaddr *)&addr, sizeof(addr)) <0) {
                close(sock); // retry
                LOG(DEBUG4) << "  Connection failed, retrying";
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }

            connected = true;
            // send my name to server
            send(sock, this->getName().c_str(), this->getName().size(), 0);


            server->setSocket(sock);
        }
    }

    return 0;
}
