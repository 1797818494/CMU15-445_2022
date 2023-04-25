//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)),
      has_no_tuple_(false) {}

void DeleteExecutor::Init() {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  printf("%d init delete %d  \n", txn->GetTransactionId(), plan_->table_oid_);
  try {
    if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_)) {
      throw ExecutionException("delete lock table");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("delete lock table");
  }
  printf("%d init delete get %d\n", txn->GetTransactionId(), plan_->table_oid_);
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  Tuple delete_tuple;
  RID delete_rid;
  int cnt = 0;
  if (has_no_tuple_) {
    return false;
  }
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    try {
      if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, delete_rid)) {
        throw ExecutionException("delete lock row");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("delete lock row");
    }
    if (table_info->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction())) {
      cnt++;
      auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto info : vec) {
        auto key = delete_tuple.KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
        info->index_->DeleteEntry(key, delete_rid, exec_ctx_->GetTransaction());
        txn->AppendIndexWriteRecord(IndexWriteRecord(delete_rid, plan_->table_oid_, WType::DELETE, delete_tuple,
                                                     info->index_oid_, exec_ctx_->GetCatalog()));
      }
      printf("txn{%d} delete rid{%u}\n", txn->GetTransactionId(), delete_rid.GetSlotNum());
      // txn->AppendTableWriteRecord(TableWriteRecord(delete_rid, WType::DELETE, delete_tuple,
      // table_info->table_.get()));
    } else {
      break;
    }
  }
  Tuple tmp(std::vector<Value>(1, Value(TypeId::INTEGER, cnt)), &plan_->OutputSchema());
  *tuple = tmp;
  has_no_tuple_ = true;
  return true;
}

}  // namespace bustub
