#include <faulttolerance/fault_tolerance.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <unordered_set>
#include <vector>

#include <data_t.hh>
#include <RequestTypes.hh>
#include <RequestWrapper.hh>

namespace pt = boost::property_tree;
namespace ft = cse498::faulttolerance;

int LOG_LEVEL = INFO;

/**
 * unfold a number of requests into a serialized array of RequestWrapper objects
 * 
 * @param keys 
 * @param prevValues 
 * @param requestTypes 
 * @param newValues 
 * @return std::vector<RequestWrapper<unsigned long long, data_t *>> 
 */
std::vector<RequestWrapper<unsigned long long, data_t *>> ft::unfoldRequest(
    std::vector<unsigned long long> keys,
    std::vector<data_t *> prevValues,
    std::vector<data_t *> newValues,
    std::vector<unsigned> requestTypes) {
    
    assert(keys.size() == prevValues.size());
    assert(keys.size() == requestTypes.size());
    assert(keys.size() == newValues.size());

    std::unordered_map<unsigned long long,
        std::pair<
            std::unordered_set<data_t *>,
            std::vector<data_t *>
        >> keyHistory;

    // Split all requests by key ignoring readonly requests
    for (int i = 0; i < keys.size(); i++) {
        if (requestTypes.at(i) == REQUEST_INSERT || requestTypes.at(i) == REQUEST_REMOVE) {
            if (keyHistory.find(keys.at(i)) == keyHistory.end()) {
                keyHistory.insert({keys.at(i),
                    std::pair<std::unordered_set<data_t *>, std::vector<data_t *>>(
                        std::unordered_set<data_t *>(),
                        std::vector<data_t *>()
                    )
                });
            }
            auto historyRecord = &keyHistory.find(keys.at(i))->second;
            historyRecord->first.insert(prevValues.at(i));

            if (historyRecord->first.find(newValues.at(i)) == historyRecord->first.end()) {
                historyRecord->second.push_back(newValues.at(i));
            }
        }
    }

    std::vector<RequestWrapper<unsigned long long, data_t *>> result;

    // Unfold requests on a per key basis
    for (auto [ key, history ] : keyHistory) {
        LOG(DEBUG4) << "Processing History for key " << key;
        unsigned int requestType = REQUEST_REMOVE;
        data_t* value = nullptr;

        for (auto newValue : history.second) {
            if (history.first.find(newValue) == history.first.end()) {
                value = newValue;
                requestType = REQUEST_INSERT;
                LOG(DEBUG4) << "Found final insert on key " << key << " value " << newValue;
                break;
            }
        }

        result.push_back({key, 0, value, requestType});
    }

    return result;
}
