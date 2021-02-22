/****************************************************
 *
 * Fault Tolerance Implementation
 *
 ****************************************************/

#ifndef KVCG_LOGGING_H
#define KVCG_LOGGING_H

#include <iostream>


enum LogLevel { ERROR, WARNING, INFO,
                DEBUG, DEBUG2, DEBUG3, DEBUG4 };
extern int LOG_LEVEL;

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
    if (opened) std::cerr << std::endl;
    opened = false;
  }

  template<class T>
  LOG &operator<<(const T &msg) {
    if (msgLevel <= LOG_LEVEL) {
        std::cerr << msg;
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

#define LOG(X) LOG(X) << __func__ <<"(): "

#endif // KVCG_LOGGING_H
