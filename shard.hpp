#ifndef CACHE_SHARD_HPP
#define CACHE_SHARD_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace cache {

/** Default number of shards for the buffer pool. */
constexpr int defaultNShards = 16;
/** Page size in bytes (fixed at compile time). */
constexpr size_t pageSize = 4096;
/** Default max cache entries per shard before eviction. */
constexpr int defaultMaxEntriesPerShard = 2048 / defaultNShards;

/** Fixed-size page buffer (pageSize bytes). */
using PageBuf = std::array<uint8_t, pageSize>;

/** Cached page: file offset (key), page data, dirty flag, and per-entry lock. */
struct Entry {
  int key{};
  PageBuf val{};
  bool isDirty = false;
  mutable std::shared_mutex mu;
};

/** Single shard of the buffer pool: in-memory page cache backed by a file. */
class Shard {
 public:
  /** Builds a shard using file descriptor \a fd and eviction limit \a maxEntriesPerShard. */
  Shard(int fd, int maxEntriesPerShard);

  /** Writes \a buf into the cached page at \a key (file offset). Loads the page if missing. Returns false on error. */
  bool Put(int key, const PageBuf& buf);

  /** Writes all dirty cached pages to the file. Returns true. */
  bool Flush();

  /** Returns the cached entry for \a key, loading from file if missing; evicts one entry if at capacity. Returns nullptr on error. */
  Entry* Get(int key);

 private:
  /** Returns the cached entry for \a key, creating it if missing. If \a loadFromFile is false, skips pread. Returns nullptr on error. */
  Entry* get(int key, bool loadFromFile);

  /** Evicts one entry from the map (caller must hold mu_). Flushes the page if dirty. Returns false on write error. */
  bool evictOneLocked();

  int fd_;
  int maxEntriesPerShard_;
  std::shared_mutex mu_;
  std::unordered_map<int, std::unique_ptr<Entry>> m_;
};

}  // namespace cache

#endif  // CACHE_SHARD_HPP
