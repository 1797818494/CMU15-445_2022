//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // printf("num frames %d k_ is %d\n", static_cast<int>(num_frames), static_cast<int>(k_));
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  bool flag = false;
  int tim = INT32_MAX;
  for (auto val = q_.rbegin(); val != q_.rend(); val++) {
    if (mp_[*val].flag_ && mp_[*val].GetFront() < tim) {
      // flag = true;
      *frame_id = *val;
      q_.erase(mp_[*frame_id].pos_);
      mp_.erase(*frame_id);
      curr_size_--;
      cnt_--;
      // printf("emit %d", *frame_id);
      return true;
    }
  }
  if (!flag) {
    for (auto val : qk_) {
      if (mp_[val].flag_ && mp_[val].GetFront() < tim) {
        flag = true;
        tim = mp_[val].GetFront();
        *frame_id = val;
      }
    }
  }
  if (flag) {
    qk_.erase(mp_[*frame_id].pos_);
    mp_.erase(*frame_id);
    curr_size_--;
    cnt_--;
    // printf("emit %d", *frame_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // printf("%d\n", frame_id);
  current_timestamp_++;
  if (mp_[frame_id].Size() < k_) {
    if (mp_[frame_id].Size() == 0) {
      q_.push_front(frame_id);
      curr_size_++;
      mp_[frame_id].pos_ = q_.begin();
      if (curr_size_ > replacer_size_) {
        printf("recordAcess abort");
        abort();
      }
    }
    mp_[frame_id].Push(current_timestamp_);
    if (mp_[frame_id].Size() == k_) {
      q_.erase(mp_[frame_id].pos_);
      qk_.push_front(frame_id);
      mp_[frame_id].pos_ = qk_.begin();
    }
  } else {
    mp_[frame_id].PushAndPop(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  // printf("setEvitable fram %d   bool : %d\n", frame_id, static_cast<int>(set_evictable));
  if (mp_[frame_id].Size() > 0 && mp_[frame_id].flag_ && !set_evictable) {
    cnt_--;
  }
  if (mp_[frame_id].Size() > 0 && !mp_[frame_id].flag_ && set_evictable) {
    cnt_++;
  }
  mp_[frame_id].flag_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // printf("Remove\n");
  if (mp_[frame_id].Size() == 0) {
    return;
  }
  if (!mp_[frame_id].flag_) {
    return;
  }
  if (mp_[frame_id].Size() == k_) {
    qk_.erase(mp_[frame_id].pos_);
    mp_.erase(frame_id);
  } else {
    q_.erase(mp_[frame_id].pos_);
    mp_.erase(frame_id);
  }
  curr_size_--;
  cnt_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return cnt_;
}

}  // namespace bustub
