#ifndef FAULT_TOLERANCE_BACKUP_PACKET_H
#define FAULT_TOLERANCE_BACKUP_PACKET_H

// TODO: Work with Networking-Layer on this
// TODO: Handle multiple keys
/**
 *
 * Packet definition for sending transaction log
 * from primary server to backup servers
 *
 */
template <typename K, typename V>
class BackupPacket {
private:
  char* serialData;

public:
  /**
   *
   * Create a packet for decoded data (sender side)
   *
   * @param key - key in table to update
   * @param value - data to store in table
   *
   */
  BackupPacket(K key, V value) { // sender side
    serialData = NULL;
    this->key = key;
    this->value = value;
  }

  /**
   *
   * Create a packet from raw data (receiver side)
   *
   * @param rawData - bytes received to be decoded
   *
   */
  BackupPacket(char* rawData) { // receiver side
      serialData = NULL;
      memcpy(&this->key, rawData, sizeof(K));
      memcpy(&this->value, rawData+sizeof(K), sizeof(V));
  }

  /**
   *
   * Destructor, free serial data if malloc'd
   *
   */
  ~BackupPacket() {
    if (serialData != NULL)
      free(serialData);
  }

  /**
   *
   * Serialize packet into raw bytes
   *
   * @return raw byte string to send on wire
   *
   */
  char* serialize() {
    if (serialData == NULL) {
      serialData = (char*) malloc(sizeof(key) + sizeof(value));
      memcpy(serialData, (char*)&key, sizeof(key));
      memcpy(serialData+sizeof(key), (char*)&value, sizeof(value));
    }
    return serialData;
  }

  /**
   *
   * Get key value of packet
   *
   * @return key
   *
   */
  K getKey() { return key; }

  /**
   *
   * Get data value of packet
   *
   * @return value
   *
   */
  V getValue() { return value; }

  /**
   *
   * Get size of packet
   *
   * @return packet size
   *
   */
  size_t getPacketSize() {
    size_t pktSize = sizeof(key) + sizeof(value);
    return pktSize;
  }

private:
  K key;
  V value;
};

#endif //FAULT_TOLERANCE_BACKUP_PACKET_H
