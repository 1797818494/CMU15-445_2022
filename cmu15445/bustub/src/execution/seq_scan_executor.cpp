//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_begin_(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->Begin(exec_ctx_->GetTransaction())),
      iter_end_(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->End()) {}

void SeqScanExecutor::Init() {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  LOG_INFO("txn{%d} init %u", txn->GetTransactionId(), plan_->GetTableOid());
  try {
    if (!txn->IsTableIntentionExclusiveLocked(plan_->GetTableOid()) &&
        txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !lock_manager->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                 plan_->GetTableOid())) {
      throw ExecutionException("Lock shared table fail");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("execute SEQ LOCK TABLA");
  }
  iter_begin_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->Begin(exec_ctx_->GetTransaction());
  iter_end_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_INFO("next %u", plan_->GetTableOid());
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  if (iter_begin_ == iter_end_) {
    if (txn->IsTableIntentionSharedLocked(plan_->table_oid_)) {
      try {
        if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !txn->GetSharedLockSet()->empty()) {
          if (!lock_manager->UnlockTable(txn, plan_->table_oid_)) {
            throw ExecutionException("unlock row shared fail");
          }
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("execute SEQ lockandunlock TABLA");
      }
    }
    return false;
  }
  try {
    if (!txn->IsRowExclusiveLocked(plan_->GetTableOid(), iter_begin_->GetRid()) &&
        txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!lock_manager->LockRow(txn, LockManager::LockMode::SHARED, plan_->table_oid_, iter_begin_->GetRid())) {
        throw ExecutionException("lock row shared fail");
      }
    }
    *tuple = *iter_begin_;
    *rid = tuple->GetRid();
    if (txn->IsRowSharedLocked(plan_->GetTableOid(), iter_begin_->GetRid()) &&
        txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (!lock_manager->UnlockRow(txn, plan_->table_oid_, iter_begin_->GetRid())) {
        throw ExecutionException("unlock row shared fail");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("execute SEQ lockandunlock TABLA");
  }
  iter_begin_++;
  return true;
}

}  // namespace bustub
