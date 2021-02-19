/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/
#include "fault_tolerance.h"
#include <iostream>
#include <sstream>


/**
 *
 * LOGGING CLASS
 *
 * TODO: Move to common repo.
 * TODO: Log to file as well.
 * TBD: std::cout vs std::cerr
 *
 * No promises on thread safety...
 *
 */
int LOG_LEVEL = INFO;
class LOG {
public:
  LOG() {}
  LOG(LogLevel l) {
    msgLevel = l;
    opened = false;
    // TODO: Add timestamp here
    operator << ("["+getLabel(l)+"]:");
  }
  ~LOG() {
    if (opened) std::cout << std::endl;
    opened = false;
  }

  template<class T>
  LOG &operator<<(const T &msg) {
    if (msgLevel <= LOG_LEVEL) {
        std::cout << msg;
        opened = true;
    }
    return *this;
  }
private:
  bool opened;
  LogLevel msgLevel;

  inline std::string getLabel(LogLevel l) {
    switch(l) {
      case ERROR: return "ERROR";
      case WARNING: return "WARNING";
      case INFO: return "INFO";
      case DEBUG: return "DEBUG";
      case DEBUG2: return "DEBUG2";
      case DEBUG3: return "DEBUG3";
      case DEBUG4: return "DEBUG4";
    }
    return "-";
  }
};


int init_server() {
    LOG(INFO) << "Initializing Server";
    return 0;
}

int init_client() {
    LOG(INFO) << "Initializing Client";
    return 0;
}
