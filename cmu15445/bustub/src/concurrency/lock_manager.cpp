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

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

#include <vector>
namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  LOG_INFO("lock table");
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
        UpdateLock(txn, lock_mode, oid);
        std::unique_lock<std::mutex> lock(queue_ptr->latch_);
        while (!GrantLock(txn, lock_mode, oid)) {
          queue_ptr->cv_.wait(lock);
        }
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
    UpdateLock(txn, lock_mode, oid);
    std::unique_lock<std::mutex> lock(queue_ptr->latch_);
    while (!GrantLock(txn, lock_mode, oid)) {
      queue_ptr->cv_.wait(lock);
    }
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
  if ((*txn->GetExclusiveRowLockSet())[oid].size() > 0 || (*txn->GetSharedRowLockSet())[oid].size()) {
    return false;
  }
  auto queue_ptr = GetLRQueuePtr(oid);
  std::lock_guard<std::mutex> lock(queue_ptr->latch_);
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
      BookKeepingRemove(txn, (*iter)->lock_mode_, oid);
      UnLockChangeState(txn, (*iter)->lock_mode_);
      // free the memory
      delete (*iter);
      queue_ptr->request_queue_.erase(iter);
      LOG_INFO("notify all");
      queue_ptr->cv_.notify_all();
      return true;
    }
  }
  TxnAbortAll(txn);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_INFO("lock row");
  auto queue_ptr = GetLRQueuePtr(oid, rid);
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    throw std::logic_error("transaction state is impossible");
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
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
        UpdateLock(txn, lock_mode, oid, rid);
        std::unique_lock<std::mutex> lock(queue_ptr->latch_);
        while (!GrantLock(txn, lock_mode, oid)) {
          queue_ptr->cv_.wait(lock);
        }
        return true;
      }
      TxnAbortAll(txn);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (LockMode::SHARED == lock_mode || LockMode::SHARED_INTENTION_EXCLUSIVE == lock_mode ||
          LockMode::INTENTION_SHARED == lock_mode) {
        TxnAbortAll(txn);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
    UpdateLock(txn, lock_mode, oid, rid);
    std::unique_lock<std::mutex> lock(queue_ptr->latch_);
    while (!GrantLock(txn, lock_mode, oid)) {
      queue_ptr->cv_.wait(lock);
    }
    return true;
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_INFO("unlock row");
  auto queue_ptr = GetLRQueuePtr(oid, rid);
  std::lock_guard<std::mutex> lock(queue_ptr->latch_);
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
      BookKeepingRemove(txn, (*iter)->lock_mode_, oid, rid);
      UnLockChangeState(txn, (*iter)->lock_mode_);
      // free the memory
      delete (*iter);
      queue_ptr->request_queue_.erase(iter);
      queue_ptr->cv_.notify_all();
      return true;
    }
  }
  TxnAbortAll(txn);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
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
  queue_ptr->latch_.lock();
  txn_id_t txn_id = txn->GetTransactionId();
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn_id) {
      if ((*iter)->lock_mode_ == lock_mode) {
        queue_ptr->latch_.unlock();
        return true;
      }
      if (queue_ptr->upgrading_ == INVALID_TXN_ID) {
        if (IsCompatible((*iter)->lock_mode_, lock_mode)) {
          queue_ptr->upgrading_ = txn_id;
          auto lock_quest_ptr = new LockRequest(txn_id, lock_mode, oid);
          // free the memory
          BookKeepingRemove(txn, (*iter)->lock_mode_, oid);
          delete (*iter);
          // to do
          queue_ptr->request_queue_.erase(iter);
          BookKeeping(txn, lock_mode, oid);
          queue_ptr->request_queue_.push_back(lock_quest_ptr);
          queue_ptr->latch_.unlock();
          return true;
        }
        LOG_INFO("imcompatible");
        queue_ptr->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      LOG_INFO("two update abort");
      txn->SetState(TransactionState::ABORTED);
      queue_ptr->latch_.unlock();
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
  }
  // no same tid in the queue
  queue_ptr->latch_.lock();
  return false;
}

auto LockManager::UpdateLock(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) -> bool {
  auto queue_ptr = GetLRQueuePtr(oid, rid);
  queue_ptr->latch_.lock();
  txn_id_t txn_id = txn->GetTransactionId();
  for (auto iter = queue_ptr->request_queue_.begin(); iter != queue_ptr->request_queue_.end(); ++iter) {
    if ((*iter)->txn_id_ == txn_id) {
      if ((*iter)->lock_mode_ == lock_mode) {
        queue_ptr->latch_.unlock();
        return true;
      }
      if (queue_ptr->upgrading_ == INVALID_TXN_ID) {
        if (IsCompatible((*iter)->lock_mode_, lock_mode)) {
          queue_ptr->upgrading_ = txn_id;
          auto lock_quest_ptr = new LockRequest(txn_id, lock_mode, oid, rid);
          // free the memory
          delete (*iter);
          queue_ptr->request_queue_.erase(iter);
          queue_ptr->request_queue_.push_back(lock_quest_ptr);
          queue_ptr->latch_.unlock();
          return true;
        }
        queue_ptr->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      txn->SetState(TransactionState::ABORTED);
      queue_ptr->latch_.unlock();
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
  }
  // no same tid in the queue
  queue_ptr->latch_.lock();
  return false;
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
  if (lm1 == LockMode::SHARED || lm2 == LockMode::INTENTION_EXCLUSIVE) {
    return false;
  }
  if (lm2 == LockMode::SHARED || lm1 == LockMode::INTENTION_EXCLUSIVE) {
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
  queue_ptr->latch_.lock();
  auto request_queue = queue_ptr->request_queue_;
  std::vector<LockMode> q;
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if ((*iter)->granted_) {
      if (!GrantCompatible((*iter)->lock_mode_, lock_mode)) {
        return false;
      }
    } else {
      if (txn->GetTransactionId() == (*iter)->txn_id_) {
        if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
          queue_ptr->upgrading_ = INVALID_TXN_ID;
          (*iter)->granted_ = true;
          BookKeeping(txn, lock_mode, oid, rid);
          return true;
        }
        for (const auto &mode : q) {
          if (!GrantCompatible(mode, lock_mode)) {
            return false;
          }
        }
        BookKeeping(txn, lock_mode, oid, rid);
        (*iter)->granted_ = true;
        return true;
      }

      q.push_back((*iter)->lock_mode_);
    }
  }
  return false;
}
auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) -> bool {
  auto queue_ptr = GetLRQueuePtr(oid);
  std::lock_guard<std::mutex> lock(queue_ptr->latch_);
  auto request_queue = queue_ptr->request_queue_;
  std::vector<LockRequest *> q;
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if ((*iter)->granted_) {
      if ((*iter)->txn_id_ == txn->GetTransactionId()) {
        LOG_INFO("granted exits");
        return true;
      }
      if (!GrantCompatible((*iter)->lock_mode_, lock_mode)) {
        return false;
      }
    } else {
      if (txn->GetTransactionId() == (*iter)->txn_id_) {
        if (queue_ptr->upgrading_ == txn->GetTransactionId()) {
          queue_ptr->upgrading_ = INVALID_TXN_ID;
          (*iter)->granted_ = true;
          // ???
          queue_ptr->cv_.notify_all();
          return true;
        }
        for (const auto &p : q) {
          if (!GrantCompatible((*p).lock_mode_, lock_mode)) {
            return false;
          }
        }
        // to do
        q.push_back((*iter));
        for (const auto &p : q) {
          (*p).granted_ = true;
        }
        queue_ptr->cv_.notify_all();
        return true;
      }

      q.push_back((*iter));
    }
  }
  return false;
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
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (mode == LockMode::EXCLUSIVE || mode == LockMode::SHARED) {
      txn->SetState(TransactionState::SHRINKING);
      return;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
      return;
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
      return;
    }
  }
}

void LockManager::TxnAbortAll(Transaction *txn) {
  // table clear
  txn->SetState(TransactionState::ABORTED);

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
}

}  // namespace bustub
