/****************************************************
 *
 * Testing for Fault Tolerance API
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <stdio.h>
#include <iostream>


int main(int argc, char const* argv[]) {

    if (argc > 1) {
        // Assume starting as client
        init_client();
    } else {
        // Assume starting as server
        init_server();
    }

    return 0;
}
