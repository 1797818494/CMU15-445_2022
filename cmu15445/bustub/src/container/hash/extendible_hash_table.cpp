
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  // printf("%d\n", static_cast<int>(idx));
  if (dir_[idx]->Insert(key, value)) {
    return;
  }
  // printf("insert");
  // idx = IndexOf(key);
  // need while ?
  bool flag = true;
  while (flag) {
    int len = dir_.size();
    idx = IndexOf(key);
    if (dir_[idx]->GetDepth() == global_depth_) {
      // dir_.resize(num_buckets_ * 2);
      global_depth_++;
      dir_[idx]->IncrementDepth();
      auto tmp = dir_[idx];
      for (int i = 0; i < len; i++) {
        if (static_cast<int>(idx) != i) {
          dir_.push_back(dir_[i]);
        } else {
          dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
        }
      }
      num_buckets_++;
      dir_[idx] = std::make_shared<Bucket>(bucket_size_, global_depth_);
      RedistributeBucket(tmp);
      auto idx1 = IndexOf(key);
      flag = !dir_[idx1]->Insert(key, value);
      // printf("%d   1\n", static_cast<int>(flag));
      continue;
    }

    if (dir_[idx]->GetDepth() < global_depth_) {
      auto tmp = dir_[idx];
      auto tmp2 = std::make_shared<Bucket>(bucket_size_, tmp->GetDepth() + 1);
      auto tmp1 = std::make_shared<Bucket>(bucket_size_, tmp->GetDepth() + 1);
      int mask = 1 << tmp->GetDepth();
      for (int i = 0; i < len; i++) {
        if (dir_[i] == tmp) {
          if (dir_[i] == tmp && i != static_cast<int>(idx) && ((mask & i) != (static_cast<int>(idx) & mask))) {
            dir_[i] = tmp1;
          } else {
            dir_[i] = tmp2;
          }
        }
      }
      num_buckets_++;
      RedistributeBucket(tmp);
      auto idx1 = IndexOf(key);
      flag = !dir_[idx1]->Insert(key, value);
      // printf("%d   1\n", static_cast<int>(flag));
      continue;
    }
  }
}  // namespace bustub

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto list = bucket->GetItems();
  auto iter = list.begin();
  while (iter != list.end()) {
    auto idx = IndexOf(iter->first);
    dir_[idx]->Insert(iter->first, iter->second);
    ++iter;
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      value = iter->second;
      return true;
    }
    ++iter;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
    ++iter;
  }
  return false;
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (key == iter->first) {
      iter->second = value;
      return true;
    }
    iter++;
  }
  if (IsFull()) {
    return false;
  }
  // mark I think right ?
  list_.push_front({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
