/****************************************************
 *
 * Testing for Fault Tolerance API
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <stdio.h>
#include <iostream>
#include <getopt.h>

void usage() {
  std::cout << "Usage: unittest_fault_tolerance [OPTIONS]" << std::endl;
  std::cout << "  -c [CONFIG] : Config JSON file (Default:" << CFG_FILE << ")" << std::endl; 
  std::cout << "  -C          : Run as client" << std::endl;
  std::cout << "  -v          : Increase verbosity" << std::endl;
  std::cout << std::endl;
}
int main(int argc, char* argv[]) {

    int opt;
    bool client = false;

    while ((opt = getopt(argc, argv, "c:Cv")) != -1) {
      switch(opt) {
        case 'c': CFG_FILE = optarg; break;
        case 'C': client = true; break;
        case 'v': LOG_LEVEL++; break;
        case '?':
            usage();
            return 1;
        default:
            usage();
            return 1;
      }
    }

    if (client) {
        init_client();
    } else {
        init_server();
    }

    return 0;
}
