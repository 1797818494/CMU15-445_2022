//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn
  // *>(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get());
}

void IndexScanExecutor::Init() {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  // std::string name = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->table_name_;
  // auto table_info = exec_ctx_->GetCatalog()->GetTable(name);
  // LOG_INFO("txn{%d} index !!!!!!!init %u", txn->GetTransactionId(), plan_->index_oid_);
  try {
    if (!txn->IsTableIntentionExclusiveLocked(table_info->oid_) &&
        txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !lock_manager->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                 table_info->oid_)) {
      throw ExecutionException("Lock sha table fail");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("execute SEQ LOCK TABLA");
  }
  auto index = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get();
  Tuple key({plan_->val_}, index->GetKeySchema());
  index->ScanKey(key, &result_, txn);
  // for(auto v : result_) {
  //   LOG_INFO("txn{%d} rid{%u}", txn->GetTransactionId(), v.GetSlotNum());
  // }
  iter_begin_ = result_.begin();
  iter_end_ = result_.end();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  std::string name = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->table_name_;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  if (iter_begin_ != iter_end_) {
    try {
      *rid = *iter_begin_;
      if (!txn->IsRowExclusiveLocked(table_info->oid_, *rid) && !txn->IsRowSharedLocked(table_info->oid_, *rid) &&
          txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (!lock_manager->LockRow(txn, LockManager::LockMode::SHARED, table_info->oid_, *rid)) {
          throw ExecutionException("index lock row shared fail");
        }
      }
      if (!exec_ctx_->GetCatalog()->GetTable(name)->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction())) {
        LOG_ERROR("not exits");
      }
      if (txn->IsRowSharedLocked(table_info->oid_, *rid)) {
        if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
          if (!lock_manager->UnlockRow(txn, table_info->oid_, *rid)) {
            throw ExecutionException("index unlock row shared fail");
          }
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("index1 execute lockandunlock TABLA");
    }
    ++iter_begin_;
    return true;
  }
  if (txn->IsTableIntentionSharedLocked(table_info->oid_)) {
    try {
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !txn->GetSharedLockSet()->empty()) {
        if (!lock_manager->UnlockTable(txn, table_info->oid_)) {
          throw ExecutionException("index unlock row shared fail");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("index2 execute  lockandunlock TABLA");
    }
  }
  return false;
}

}  // namespace bustub
