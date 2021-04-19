#include <faulttolerance/fault_tolerance.h>
#include <faulttolerance/shard.h>
#include <networklayer/connection.hh>

int ft::Shard::discoverPrimary() {
    int status = KVCG_ESUCCESS;
    bool found = false;
    int offset;
    size_t numRanges;
    unsigned long long minKey, maxKey;
    uint64_t mrkey = 0;
    LOG(INFO) << "Discovering primary for shard [" << this->getLowerBound() << ", " << this->getUpperBound() << "]";

    // Try to establish connection to each server in shard (non-blocking, assume down if unable)
    for (auto server : this->getServers()) {
        server->primary_conn = new cse498::Connection(server->getAddr().c_str(), false, server->getClientPort(), server->getProvider());
        if(!server->primary_conn->connect()) {
            // TBD: is connect() blocking? is there an alternative?
            delete server->primary_conn;
            continue;
        }
        cse498::unique_buf resp(4096);
        server->primary_conn->register_mr(resp, FI_SEND | FI_RECV, mrkey);
        server->primary_conn->recv(resp, 4096);
        memcpy(&numRanges, resp.get(), sizeof(size_t)); 

        // TODO: Calculate if number of ranges creates buffer that is >4096 and handle
        offset = sizeof(size_t);
        for (int i=0; i < numRanges; i++) {
            memcpy(&minKey, resp.get()+offset, sizeof(unsigned long long));
            offset += sizeof(unsigned long long);
            memcpy(&maxKey, resp.get()+offset, sizeof(unsigned long long));
            offset += sizeof(unsigned long long);
            if (minKey == this->getLowerBound() && maxKey == this->getUpperBound()) {
                LOG(INFO) << "Found primary " << server->getName();
                this->setPrimary(server);
                found = true;
                break;
            }
        }

        delete server->primary_conn;

        if (found) break;
    }

    if (!found) {
        LOG(ERROR) << "Could not find primary server for shard";
        status = KVCG_EUNKNOWN;
    }

    return status;
}