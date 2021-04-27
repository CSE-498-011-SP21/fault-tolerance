#include <chrono>
#include <thread>
#include <iostream>
#include <vector>
#include <map>
#include <faulttolerance/fault_tolerance.h>
#include <data_t.hh>
#include <gtest/gtest.h>

namespace ft = cse498::faulttolerance;

std::string cfgFile = "gtest_kvcg.json";
 
TEST(ftTest, single_logRequest) {
    LOG_LEVEL = DEBUG;
    ft::Server* server  = new ft::Server();

    EXPECT_EQ(0, server->initialize(cfgFile));

    data_t* value = new data_t(5);
    value->data = (char*)"word";
    EXPECT_EQ(0, server->logRequest(3, value));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    delete server;

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(ftTest, multi_put) {
    LOG_LEVEL = DEBUG;
    ft::Server* server = new ft::Server();
    EXPECT_EQ(0, server->initialize(cfgFile));

    std::vector<unsigned long long> keys = { 3, 5, 20, 6, 100 };
    std::vector<data_t*> values;

    for(int i=0; i < keys.size(); i++) {
      values.push_back(new data_t(6));
      std::string value = "word" + std::to_string(i);
      memcpy(values.at(i)->data, value.c_str(), 5);
    }

    EXPECT_EQ(0, server->logRequest(keys, values));

   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
   delete server;
   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(ftTest, bad_multi_put) {
    LOG_LEVEL = DEBUG;
    ft::Server* server = new ft::Server();
    EXPECT_EQ(0, server->initialize(cfgFile));

    std::vector<unsigned long long> keys = { 3, 5, 2000, 6, 1001 };
    std::vector<data_t*> values;

    for(int i=0; i < keys.size(); i++) {
      values.push_back(new data_t(6));
      std::string value = "word" + std::to_string(i);
      memcpy(values.at(i)->data, value.c_str(), 5);
    }

    EXPECT_NE(0, server->logRequest(keys, values));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    delete server;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(ftTest, batch_put) {
    LOG_LEVEL = DEBUG;
    ft::Server* server = new ft::Server();
    EXPECT_EQ(0, server->initialize(cfgFile));
    std::vector<RequestWrapper<unsigned long long, data_t *>> batch;

    for (unsigned long long i=0; i<512; i++) {
        std::string valueStr = "word" + std::to_string(i);
        data_t* value = new data_t(valueStr.length()+1);
        memcpy(value->data, valueStr.c_str(), valueStr.length());
        value->data[valueStr.length()] = '\0';
        RequestWrapper<unsigned long long, data_t*> pkt{i, 0, value, REQUEST_INSERT};
        batch.push_back(pkt);
    }

    EXPECT_EQ(0, server->logRequest(batch));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    delete server;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(ftTest, batch_mixed) {
    LOG_LEVEL = DEBUG;
    ft::Server* server = new ft::Server();
    EXPECT_EQ(0, server->initialize(cfgFile));
    std::vector<RequestWrapper<unsigned long long, data_t *>> batch;

    for (unsigned long long i=0; i<512; i++) {
        std::string valueStr = "word" + std::to_string(i);
        data_t* value = new data_t(valueStr.length()+1);
        memcpy(value->data, valueStr.c_str(), valueStr.length());
        value->data[valueStr.length()] = '\0';
        unsigned int requestInt = REQUEST_INSERT;
        if (i % 100 == 0) {
          requestInt = REQUEST_REMOVE;
        } else if (i % 50 == 0) {
          requestInt = REQUEST_GET;
        }
        RequestWrapper<unsigned long long, data_t*> pkt{i, 0, value, requestInt};
        batch.push_back(pkt);
    }

    EXPECT_EQ(0, server->logRequest(batch));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    delete server;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(ftTest, bad_batch) {
    LOG_LEVEL = DEBUG2;
    ft::Server* server = new ft::Server();
    EXPECT_EQ(0, server->initialize(cfgFile));
    std::vector<RequestWrapper<unsigned long long, data_t *>> batch;
    std::vector<RequestWrapper<unsigned long long, data_t *>> failedBatch;

    for (unsigned long long i=900; i<1003; i++) {
        std::string valueStr = "word" + std::to_string(i);
        data_t* value = new data_t(valueStr.length()+1);
        memcpy(value->data, valueStr.c_str(), valueStr.length());
        value->data[valueStr.length()] = '\0';
        unsigned int requestInt = REQUEST_INSERT;
        if (i % 100 == 0) {
          requestInt = REQUEST_REMOVE;
        } else if (i % 50 == 0) {
          requestInt = REQUEST_GET;
        }
        RequestWrapper<unsigned long long, data_t*> pkt{i, 0, value, requestInt};
        batch.push_back(pkt);
    }

    EXPECT_NE(0, server->logRequest(batch, &failedBatch));

    EXPECT_EQ(2, failedBatch.size());
    for (auto f : failedBatch) {
        std::cout << "Failed - " << f.key << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    delete server;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(ftTest, unfold_requests) {
    LOG_LEVEL = DEBUG2;
    ft::Server* server = new ft::Server();
    std::unordered_map<unsigned long long, data_t *> store;

    size_t opCount = 10000;

    auto order = std::vector<size_t>(opCount);
    for (auto i = 0; i < order.size(); i++) {
        order.at(i) = i;
    }
    std::random_shuffle(order.begin(), order.end());

    auto keys = std::vector<unsigned long long>(opCount);
    auto oldValues = std::vector<data_t*>(opCount);
    auto newValues = std::vector<data_t*>(opCount);
    auto requestTypes = std::vector<unsigned>(opCount);

    srand(0);
    for (int i = 0; i < opCount; i++) {
        size_t index = order.at(i);
        unsigned long long key = rand() % 10;
        keys.at(index) = key;
        
        auto oldVal = store.find(key);
        if (oldVal != store.end()) {
            oldValues.at(index) = oldVal->second;
        }
        else {
            oldValues.at(index) = nullptr;
        }

        if (rand() % 10 < 8) {
            newValues.at(index) = new data_t(4);
            requestTypes.at(index) = REQUEST_INSERT;
        }
        else {
            newValues.at(index) = nullptr;
            requestTypes.at(index) = REQUEST_REMOVE;
        }

        LOG(DEBUG4) << "Inserting " << newValues.at(index) << " at key " << key;

        if (store.find(key) == store.end()) {
            store.insert({key, newValues.at(index)});
        } else {
            store.find(key)->second = newValues.at(index);
        }
    }

    auto results = server->unfoldRequest(keys, oldValues, newValues, requestTypes);

    for (auto result : results) {
        auto storedElement = store.find(result.key);
        if (storedElement != store.end()) {
            EXPECT_EQ(result.value, storedElement->second);
        }
        else {
            LOG(DEBUG2) << "Unexpect key returned from unfoldRequest";
        }
    }
}