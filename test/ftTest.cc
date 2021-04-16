#include <chrono>
#include <thread>
#include <iostream>
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
    value->data = "word";
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

    for (int i=0; i<512; i++) {
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

    for (int i=0; i<512; i++) {
        std::string valueStr = "word" + std::to_string(i);
        data_t* value = new data_t(valueStr.length()+1);
        memcpy(value->data, valueStr.c_str(), valueStr.length());
        value->data[valueStr.length()] = '\0';
        int requestInt = REQUEST_INSERT;
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

