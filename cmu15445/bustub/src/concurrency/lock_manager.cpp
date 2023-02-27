//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <stack>
#include <unordered_map>
#include <vector>

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  LOG_INFO("lock table");
  Log(txn, lock_mode, oid);
  auto queue_ptr = GetLRQueuePtr(oid);
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    throw std::logic_error("transaction state is impossible");
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    LOG_INFO("shringkong");
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        TxnAbortAll(txn);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (LockMode::SHARED == lock_mode || lock_mode == LockMode::INTENTION_SHARED) {
        if (!UpdateLock(txn, lock_mode, oid)) {
          LOG_INFO("the same fail");
          return false;
        }
        std::unique_lock<std::mutex> lock(queue_ptr->latch_);
        while (!GrantLock(txn, lock_mode, oid)) {
          queue_ptr->cv_.wait(lock);
          if (txn->GetState() == TransactionState::ABORTED) {
            LOG_INFO("granting abort!!!");
            auto &q = queue_ptr->request_queue_;
            if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
              queue_ptr->upgrading_ = INVALID_TXN_ID;
            }
            for (auto iter = q.begin(); iter != q.end(); ++iter) {
              if ((*iter)->txn_id_ == txn->GetTransactionId()) {
                q.erase(iter);
                queue_ptr->cv_.notify_all();
                return false;
              }
            }
          }
        }
        queue_ptr->cv_.notify_all();
        BookKeeping(txn, lock_mode, oid);
        LOG_INFO("blocking finish");
        return true;
      }
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    LOG_INFO("growing");
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (LockMode::SHARED == lock_mode || LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode ||
          LockMode::INTENTION_SHARED == lock_mode) {
        TxnAbortAll(txn);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
    if (!UpdateLock(txn, lock_mode, oid)) {
      LOG_INFO("the same fail");
      return false;
    }
    std::unique_lock<std::mutex> lock(queue_ptr->latch_);
    while (!GrantLock(txn, lock_mode, oid)) {
      queue_ptr->cv_.wait(lock);
      Log(txn, lock_mode, oid);
      if (txn->GetState() == TransactionState::ABORTED) {
        LOG_INFO("granting abort!!!");
        auto &q = queue_ptr->request_queue_;
        if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
          queue_ptr->upgrading_ = INVALID_TXN_ID;
        }
        for (auto iter = q.begin(); iter != q.end(); ++iter) {
          if ((*iter)->txn_id_ == txn->GetTransactionId()) {
            q.remove((*iter));
            LOG_INFO("q.size() is %ld", q.size());
            queue_ptr->cv_.notify_all();
            return false;
          }
        }
      }
    }
    queue_ptr->cv_.notify_all();
    BookKeeping(txn, lock_mode, oid);
    LOG_INFO("blocking finish");
    return true;
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // to do check row lock
  // to check
  LOG_INFO("unlock table");
  Log(txn, LockMode::EXCLUSIVE, oid);
  if ((*txn->GetExclusiveRowLockSet())[oid].size() > 0 || (*txn->GetSharedRowLockSet())[oid].size() > 0) {
    LOG_ERROR("the row can't unlock");
    TxnAbortAll(txn);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  auto queue_ptr = GetLRQueuePtr(oid);
  std::lock_guard<std::mutex> lock(queue_ptr->latch_);
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
      BookKeepingRemove(txn, (*iter)->lock_mode_, oid);
      UnLockChangeState(txn, (*iter)->lock_mode_);
      // free the memory
      queue_ptr->request_queue_.erase(iter);
      LOG_INFO("notify all");
      queue_ptr->cv_.notify_all();
      return true;
    }
  }
  LOG_ERROR("abort");
  TxnAbortAll(txn);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}
void LockManager::IsTableFit(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    return;
  }
  if (lock_mode == LockMode::SHARED) {
    if (!(txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
          txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    return;
  }
  TxnAbortAll(txn);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
}
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_INFO("lock row");
  // judge the table
  Log(txn, lock_mode, oid);
  IsTableFit(txn, lock_mode, oid);
  auto queue_ptr = GetLRQueuePtr(oid, rid);
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    throw std::logic_error("transaction state is impossible");
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    LOG_INFO("shringkong");
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        TxnAbortAll(txn);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (LockMode::SHARED == lock_mode || lock_mode == LockMode::INTENTION_SHARED) {
        if (!UpdateLock(txn, lock_mode, oid, rid)) {
          LOG_INFO("the same fail");
          return false;
        }
        std::unique_lock<std::mutex> lock(queue_ptr->latch_);
        while (!GrantLock(txn, lock_mode, oid, rid)) {
          queue_ptr->cv_.wait(lock);
          if (txn->GetState() == TransactionState::ABORTED) {
            LOG_INFO("granting abort!!!");
            auto &q = queue_ptr->request_queue_;
            if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
              queue_ptr->upgrading_ = INVALID_TXN_ID;
            }
            for (auto iter = q.begin(); iter != q.end(); ++iter) {
              if ((*iter)->txn_id_ == txn->GetTransactionId()) {
                q.erase(iter);
                queue_ptr->cv_.notify_all();
                return false;
              }
            }
          }
        }
        queue_ptr->cv_.notify_all();
        BookKeeping(txn, lock_mode, oid, rid);
        LOG_INFO("blocking finish");
        return true;
      }
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    LOG_INFO("growing");
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (LockMode::SHARED == lock_mode || LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode ||
          LockMode::INTENTION_SHARED == lock_mode) {
        TxnAbortAll(txn);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
    if (!UpdateLock(txn, lock_mode, oid, rid)) {
      LOG_INFO("the same fail");
      return false;
    }
    std::unique_lock<std::mutex> lock(queue_ptr->latch_);
    while (!GrantLock(txn, lock_mode, oid, rid)) {
      queue_ptr->cv_.wait(lock);
      if (txn->GetState() == TransactionState::ABORTED) {
        LOG_INFO("granting abort!!!");
        auto &q = queue_ptr->request_queue_;
        if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
          queue_ptr->upgrading_ = INVALID_TXN_ID;
        }
        for (auto iter = q.begin(); iter != q.end(); ++iter) {
          if ((*iter)->txn_id_ == txn->GetTransactionId()) {
            q.erase(iter);
            queue_ptr->cv_.notify_all();
            return false;
          }
        }
      }
    }
    queue_ptr->cv_.notify_all();
    BookKeeping(txn, lock_mode, oid, rid);
    LOG_INFO("blocking finish");
    return true;
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_INFO("unlock row");
  Log(txn, LockMode::EXCLUSIVE, oid);
  // to do !!!!!!!!!!!!!!!!!!!!!!!!
  auto queue_ptr = GetLRQueuePtr(oid, rid);
  std::lock_guard<std::mutex> lock(queue_ptr->latch_);
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
      BookKeepingRemove(txn, (*iter)->lock_mode_, oid, rid);
      UnLockChangeState(txn, (*iter)->lock_mode_);
      // free the memory
      queue_ptr->request_queue_.erase(iter);
      queue_ptr->cv_.notify_all();
      return true;
    }
  }
  LOG_INFO("unlock fail %d !!!!!!!!!!!!!!!!", txn->GetTransactionId());
  TxnAbortAll(txn);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  LOG_INFO("add %d %d", t1, t2);
  std::lock_guard<std::mutex> lock(waits_for_latch_);
  for (const auto &t : waits_for_[t1]) {
    if (t == t2) {
      return;
    }
  }
  mp_[t1]++;
  mp_[t2]++;
  waits_for_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  LOG_INFO("remove %d %d", t1, t2);
  std::lock_guard<std::mutex> lock(waits_for_latch_);
  for (auto iter = waits_for_[t1].begin(); iter != waits_for_[t1].end(); ++iter) {
    if ((*iter) == t2) {
      waits_for_[t1].erase(iter);
      return;
    }
  }
}
auto LockManager::Dfs(txn_id_t txn_id, std::unordered_map<txn_id_t, int> &mp, std::stack<txn_id_t> &stk,
                      std::unordered_map<txn_id_t, int> &ump) -> bool {
  stk.push(txn_id);
  mp[txn_id] = 1;
  ump[txn_id] = -1;
  for (auto txn : waits_for_[txn_id]) {
    if (mp[txn] == 1) {
      return true;
    }
    if (Dfs(txn, mp, stk, ump)) {
      return true;
    }
  }
  stk.pop();
  mp[txn_id] = 0;
  return false;
}
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_map<txn_id_t, int> mp;
  std::stack<txn_id_t> stk;
  txn_id_t txn_min = INT_MAX;
  for (auto [key, val] : mp_) {
    if (val > 0) {
      txn_min = std::min(txn_min, key);
    }
  }
  LOG_INFO("txn min is %d", txn_min);
  if (txn_min == INT_MAX) {
    return false;
  }
  std::unordered_map<txn_id_t, int> ump;
  while (!Dfs(txn_min, mp, stk, ump)) {
    txn_min = INT_MAX;
    for (auto [key, val] : mp_) {
      if (ump[key] != -1 && val > 0) {
        txn_min = std::min(txn_min, key);
      }
    }
    if (txn_min == INT_MAX) {
      LOG_INFO("has cycle false");
      return false;
    }
  }
  auto txn_start = stk.top();
  stk.pop();
  while (!stk.empty() && stk.top() != txn_start) {
    LOG_INFO("%d  .......", txn_start);
    txn_start = std::max(txn_start, stk.top());
    stk.pop();
  }
  *txn_id = txn_start;
  LOG_INFO("has cycle true");
  mp_[txn_start] = 0;
  return true;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  std::lock_guard<std::mutex> lock(waits_for_latch_);
  for (const auto &[key, vec] : waits_for_) {
    for (const auto &val : vec) {
      edges.push_back({key, val});
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // build
      std::lock_guard<std::mutex> lock1(waits_for_latch_);
      // table add edge
      table_lock_map_latch_.lock();
      auto my_table = table_lock_map_;
      table_lock_map_latch_.unlock();
      for (const auto &[key, que] : my_table) {
        que->latch_.lock();
        auto &q = que->request_queue_;
        for (auto iter = q.begin(); iter != q.end(); ++iter) {
          auto iter1 = iter;
          iter1++;
          for (; iter1 != q.end(); ++iter1) {
            if (!GrantCompatible((*iter)->lock_mode_, (*iter1)->lock_mode_)) {
              if ((*iter)->granted_ && !(*iter1)->granted_) {
                waits_for_[(*iter)->txn_id_].insert((*iter1)->txn_id_);
                mp_[(*iter1)->txn_id_] = 1;
                mp_[(*iter)->txn_id_] = 1;
              }
              if (!(*iter)->granted_ && (*iter1)->granted_) {
                waits_for_[(*iter1)->txn_id_].insert((*iter)->txn_id_);
                mp_[(*iter1)->txn_id_] = 1;
                mp_[(*iter)->txn_id_] = 1;
              }
            }
          }
        }
        que->latch_.unlock();
      }
      // table_lock_map_latch_.unlock();

      // row add edges
      row_lock_map_latch_.lock();
      auto my_row = row_lock_map_;
      row_lock_map_latch_.unlock();
      for (const auto &[key, que] : my_row) {
        que->latch_.lock();
        auto &q = que->request_queue_;
        for (auto iter = q.begin(); iter != q.end(); ++iter) {
          auto iter1 = iter;
          iter1++;
          for (; iter1 != q.end(); ++iter1) {
            if (!GrantCompatible((*iter)->lock_mode_, (*iter1)->lock_mode_)) {
              if ((*iter)->granted_ && !(*iter1)->granted_) {
                waits_for_[(*iter1)->txn_id_].insert((*iter)->txn_id_);
                mp_[(*iter1)->txn_id_] = 1;
                mp_[(*iter)->txn_id_] = 1;
              }
              if (!(*iter)->granted_ && (*iter1)->granted_) {
                waits_for_[(*iter)->txn_id_].insert((*iter1)->txn_id_);
                mp_[(*iter1)->txn_id_] = 1;
                mp_[(*iter)->txn_id_] = 1;
              }
            }
          }
        }
        que->latch_.unlock();
      }
      // row_lock_map_latch_.unlock();
      txn_id_t ans;
      // my add
      std::vector<std::pair<txn_id_t, txn_id_t>> edges;
      for (const auto &[key, vec] : waits_for_) {
        for (const auto &val : vec) {
          edges.push_back({key, val});
        }
      }
      for (const auto &[t1, t2] : edges) {
        LOG_INFO("%d %d", t1, t2);
      }
      while (HasCycle(&ans)) {
        Transaction *txn = TransactionManager::GetTransaction(ans);
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        LOG_INFO("%d is aborted !!!", ans);
        waits_for_.erase(ans);
        for (auto &[key, vec] : waits_for_) {
          vec.erase(ans);
        }
        table_lock_map_latch_.lock();
        auto my_table1 = table_lock_map_;
        table_lock_map_latch_.unlock();
        for (auto &[table_id, quest_lock] : my_table1) {
          std::lock_guard<std::mutex> lock4(quest_lock->latch_);
          for (auto iter = quest_lock->request_queue_.begin(); iter != quest_lock->request_queue_.end();) {
            if ((*iter)->txn_id_ == ans && (*iter)->granted_) {
              iter = quest_lock->request_queue_.erase(iter);
            } else {
              iter++;
            }
          }
          quest_lock->cv_.notify_all();
        }
        // table_lock_map_latch_.unlock();

        row_lock_map_latch_.lock();
        auto my_row1 = row_lock_map_;
        row_lock_map_latch_.unlock();
        for (auto &[table_id, quest_lock] : my_row1) {
          std::lock_guard<std::mutex> lock4(quest_lock->latch_);
          for (auto iter = quest_lock->request_queue_.begin(); iter != quest_lock->request_queue_.end();) {
            if ((*iter)->txn_id_ == ans && (*iter)->granted_) {
              iter = quest_lock->request_queue_.erase(iter);
            } else {
              iter++;
            }
          }
          quest_lock->cv_.notify_all();
        }
        txn->LockTxn();
        txn->GetSharedTableLockSet()->clear();
        txn->GetExclusiveTableLockSet()->clear();
        txn->GetIntentionExclusiveTableLockSet()->clear();
        txn->GetIntentionSharedTableLockSet()->clear();
        txn->GetSharedIntentionExclusiveTableLockSet()->clear();

        // row clear
        txn->GetExclusiveLockSet()->clear();
        txn->GetSharedLockSet()->clear();

        // row and table map clear
        txn->GetSharedRowLockSet()->clear();
        txn->GetExclusiveRowLockSet()->clear();
        txn->UnlockTxn();
      }
      waits_for_.clear();
      mp_.clear();
    }
  }
}

// my add
auto LockManager::GetLRQueuePtr(table_oid_t id, RID rid) -> std::shared_ptr<LockRequestQueue> {
  std::lock_guard<std::mutex> lock(row_lock_map_latch_);
  auto ptr = row_lock_map_[rid];
  if (ptr.get() == nullptr) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    return row_lock_map_[rid];
  }
  return ptr;
}
auto LockManager::GetLRQueuePtr(table_oid_t id) -> std::shared_ptr<LockRequestQueue> {
  std::lock_guard<std::mutex> lock(table_lock_map_latch_);
  auto ptr = table_lock_map_[id];
  if (ptr.get() == nullptr) {
    table_lock_map_[id] = std::make_shared<LockRequestQueue>();
    return table_lock_map_[id];
  }
  return ptr;
}

auto LockManager::IsCompatible(LockMode lock_mode1, LockMode lock_mode) -> bool {
  if (lock_mode1 == LockMode::INTENTION_SHARED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
       lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (lock_mode1 == LockMode::SHARED &&
      (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (lock_mode1 == LockMode::INTENTION_EXCLUSIVE &&
      (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }
  if (lock_mode1 == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE) {
    return true;
  }
  return false;
}
auto LockManager::UpdateLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) -> bool {
  auto queue_ptr = GetLRQueuePtr(oid);
  std::lock_guard<std::mutex> lock(queue_ptr->latch_);
  txn_id_t txn_id = txn->GetTransactionId();
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn_id) {
      if ((*iter)->lock_mode_ == lock_mode) {
        return false;
      }
      if (queue_ptr->upgrading_ == INVALID_TXN_ID) {
        if (IsCompatible((*iter)->lock_mode_, lock_mode)) {
          queue_ptr->upgrading_ = txn_id;
          // free the memory
          BookKeepingRemove(txn, (*iter)->lock_mode_, oid);
          // to do
          LOG_INFO("update success");
          queue_ptr->request_queue_.erase(iter);
          auto lock_quest_ptr = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
          queue_ptr->request_queue_.push_back(lock_quest_ptr);
          return true;
        }
        LOG_INFO("imcompatible");
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      LOG_INFO("two update abort");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
  }
  LOG_INFO("no same");
  // no same tid in the queue
  auto lock_quest_ptr = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  queue_ptr->request_queue_.push_back(lock_quest_ptr);
  return true;
}

auto LockManager::UpdateLock(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) -> bool {
  auto queue_ptr = GetLRQueuePtr(oid, rid);
  std::lock_guard<std::mutex> lock(queue_ptr->latch_);
  txn_id_t txn_id = txn->GetTransactionId();
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn_id) {
      if ((*iter)->lock_mode_ == lock_mode) {
        return false;
      }
      if (queue_ptr->upgrading_ == INVALID_TXN_ID) {
        if (IsCompatible((*iter)->lock_mode_, lock_mode)) {
          queue_ptr->upgrading_ = txn_id;
          // free the memory
          BookKeepingRemove(txn, (*iter)->lock_mode_, oid, rid);
          // to do
          LOG_INFO("update success");
          queue_ptr->request_queue_.erase(iter);
          auto lock_quest_ptr = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
          queue_ptr->request_queue_.push_back(lock_quest_ptr);
          return true;
        }
        LOG_INFO("imcompatible");
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      LOG_INFO("two update abort");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
  }
  LOG_INFO("no same");
  // no same tid in the queue
  auto lock_quest_ptr = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  queue_ptr->request_queue_.push_back(lock_quest_ptr);
  return true;
}
// auto LockManager::GetTxnMode(Transaction* txn, table_oid_t oid, RID rid) -> LockMode {
//   if(txn->IsRowExclusiveLocked(oid, rid)) {
//     return LockMode::EXCLUSIVE;
//   }
//   if(txn->IsRowSharedLocked(oid, rid)) {
//     return LockMode::SHARED;
//   }
//   throw std::logic_error("lock state is impossible");
// }
// auto LockManager::GetTxnMode(Transaction* txn, table_oid_t oid) -> LockMode {
//    if(txn->IsTableExclusiveLocked(oid)) {
//       return LockMode::EXCLUSIVE;
//    }
//    if(txn->IsTableSharedIntentionExclusiveLocked(oid)) {
//      return LockMode::SHARED_INTENTION_EXCLUSIVE;
//    }
//    if(txn->IsTableSharedLocked(oid)) {
//     return LockMode::SHARED;
//    }
//    if(txn->IsTableIntentionSharedLocked(oid)) {
//     return LockMode::INTENTION_SHARED;
//    }
//    if(txn->IsTableIntentionExclusiveLocked(oid)) {
//     return LockMode::INTENTION_EXCLUSIVE;
//    }
//    LOG_INFO("lock state is impossible");
//    throw std::logic_error("lock state is impossible");
// }
auto LockManager::GrantCompatible(LockMode lm1, LockMode lm2) -> bool {
  if (lm1 == LockMode::EXCLUSIVE || lm2 == LockMode::EXCLUSIVE) {
    return false;
  }
  if (lm1 == LockMode::SHARED && lm2 == LockMode::INTENTION_EXCLUSIVE) {
    return false;
  }
  if (lm2 == LockMode::SHARED && lm1 == LockMode::INTENTION_EXCLUSIVE) {
    return false;
  }
  if (lm1 == LockMode::SHARED_INTENTION_EXCLUSIVE && lm2 != LockMode::INTENTION_SHARED) {
    return false;
  }
  if (lm2 == LockMode::SHARED_INTENTION_EXCLUSIVE && lm1 != LockMode::INTENTION_SHARED) {
    return false;
  }
  return true;
}
auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) -> bool {
  auto queue_ptr = GetLRQueuePtr(oid, rid);
  auto request_queue = queue_ptr->request_queue_;
  if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
    for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
      if ((*iter)->granted_) {
        if (!GrantCompatible((*iter)->lock_mode_, lock_mode)) {
          LOG_INFO("update fail");
          return false;
        }
      }
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        queue_ptr->upgrading_ = INVALID_TXN_ID;
        (*iter)->granted_ = true;
        LOG_INFO("update grant");
        return true;
      }
    }
    LOG_INFO("update fail");
    return false;
  }
  std::vector<std::shared_ptr<LockRequest>> q;
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if ((*iter)->granted_) {
      q.push_back((*iter));
    }
  }
  bool flag = false;
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if (!(*iter)->granted_) {
      for (const auto &ptr : q) {
        if (!GrantCompatible(ptr->lock_mode_, (*iter)->lock_mode_)) {
          LOG_INFO("grant return %i %d", flag, txn->GetTransactionId());
          return flag;
        }
      }
      (*iter)->granted_ = true;
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        flag = true;
      }
      q.push_back((*iter));
    } else {
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        flag = true;
      }
    }
  }
  LOG_INFO("grant return %i %d", flag, txn->GetTransactionId());
  return flag;
}
auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) -> bool {
  auto queue_ptr = GetLRQueuePtr(oid);
  LOG_INFO("queue is %ld", queue_ptr->request_queue_.size());
  auto request_queue = queue_ptr->request_queue_;
  if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
    for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
      if ((*iter)->lock_mode_ != lock_mode && (*iter)->txn_id_ == txn->GetTransactionId()) {
        continue;
      }
      if ((*iter)->granted_) {
        if (!GrantCompatible((*iter)->lock_mode_, lock_mode)) {
          LOG_INFO("update fail");
          return false;
        }
      }
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        queue_ptr->upgrading_ = INVALID_TXN_ID;
        (*iter)->granted_ = true;
        LOG_INFO("update grant");
        return true;
      }
    }
    LOG_INFO("update fail");
    return false;
  }
  Log(txn, lock_mode, oid);
  std::vector<std::shared_ptr<LockRequest>> q;
  LOG_INFO("%d", (int)request_queue.size());
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if ((*iter)->granted_) {
      q.push_back((*iter));
    }
  }
  bool flag = false;
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if (!(*iter)->granted_) {
      for (const auto &ptr : q) {
        if (!GrantCompatible(ptr->lock_mode_, (*iter)->lock_mode_)) {
          LOG_INFO("grant return %i %d", flag, txn->GetTransactionId());
          return flag;
        }
      }
      (*iter)->granted_ = true;
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        flag = true;
      }
      q.push_back((*iter));
    } else {
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        flag = true;
      }
    }
  }
  LOG_INFO("grant return %i %d", flag, txn->GetTransactionId());
  return flag;
}

void LockManager::BookKeeping(Transaction *txn, LockMode mode, table_oid_t oid, RID rid) {
  if (mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
  if (mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  }
}
void LockManager::BookKeepingRemove(Transaction *txn, LockMode mode, table_oid_t oid, RID rid) {
  if (mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }
  if (mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  }
}

void LockManager::BookKeeping(Transaction *txn, LockMode mode, table_oid_t oid) {
  if (mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  }
  if (mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  }
  if (mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  }
  if (mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
  if (mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  }
}

void LockManager::BookKeepingRemove(Transaction *txn, LockMode mode, table_oid_t oid) {
  if (mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  }
  if (mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  }
  if (mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  }
  if (mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
  if (mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  }
}
void LockManager::UnLockChangeState(Transaction *txn, LockMode mode) {
  if (txn->GetState() != TransactionState::GROWING) {
    return;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (mode == LockMode::EXCLUSIVE || mode == LockMode::SHARED) {
      LOG_INFO("start shringking");
      txn->SetState(TransactionState::SHRINKING);
      return;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
      LOG_INFO("start shringking");
      return;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
      LOG_INFO("start shringking");
      return;
    }
  }
}

void LockManager::TxnAbortAll(Transaction *txn) {
  // table clear
  txn->SetState(TransactionState::ABORTED);

  // txn->GetSharedTableLockSet()->clear();
  // txn->GetExclusiveTableLockSet()->clear();
  // txn->GetIntentionExclusiveTableLockSet()->clear();
  // txn->GetIntentionSharedTableLockSet()->clear();
  // txn->GetSharedIntentionExclusiveTableLockSet()->clear();

  // // row clear
  // txn->GetExclusiveLockSet()->clear();
  // txn->GetSharedLockSet()->clear();

  // // row and table map clear
  // txn->GetSharedRowLockSet()->clear();
  // txn->GetExclusiveRowLockSet()->clear();
}

}  // namespace bustub
