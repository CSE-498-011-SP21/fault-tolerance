#ifndef FAULT_TOLERANCE_DEFINITIONS_H
#define FAULT_TOLERANCE_DEFINITIONS_H

#include <iostream>

#define key_t uint64_t
#define data_t uint64_t

// struct data_t {

//     data_t() : size(0), data(nullptr) {}

//     data_t(size_t s) : size(s), data(new char[s]) {}

//     /// Note this doesn't free the underlying data
//     ~data_t() {}

//     size_t size;
//     char *data;

//     data_t &operator=(const data_t &rhs) {
//         this->size = rhs.size;
//         this->data = rhs.data;
//         return *this;
//     }

//     volatile data_t &operator=(const data_t &rhs) volatile {
//         this->size = rhs.size;
//         this->data = rhs.data;
//         return *this;
//     }

// };

#endif // FAULT_TOLERANCE_DEFINITIONS