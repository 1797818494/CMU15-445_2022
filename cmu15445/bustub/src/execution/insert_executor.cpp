//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include <memory>
#include <vector>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)),
      has_no_tuple_(false) {}

void InsertExecutor::Init() {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  printf("%d init insert %d\n", txn->GetTransactionId(), plan_->table_oid_);
  try {
    if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_)) {
      throw ExecutionException("insert lock table");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("insert lock table");
  }
  printf("%d init insert get %d\n", txn->GetTransactionId(), plan_->table_oid_);
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple insert_tuple;
  RID insert_rid;
  int cnt = 0;
  if (has_no_tuple_) {
    return false;
  }
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(&insert_tuple, &insert_rid)) {
    TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    try {
      if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, insert_rid)) {
        throw ExecutionException("insert lock row");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("insert lock row");
    }
    if (table_info->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction())) {
      // printf("txn{%d} insert rid{%u} page{%d}\n", txn->GetTransactionId(), insert_rid.GetSlotNum(),
      // insert_rid.GetPageId());
      cnt++;
      auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto info : vec) {
        auto key = insert_tuple.KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
        info->index_->InsertEntry(key, insert_rid, exec_ctx_->GetTransaction());
        txn->AppendIndexWriteRecord(IndexWriteRecord(insert_rid, plan_->table_oid_, WType::INSERT, insert_tuple,
                                                     info->index_oid_, exec_ctx_->GetCatalog()));
      }
    } else {
      LOG_INFO("insert fail");
      break;
    }
  }
  // NULL to be check
  Tuple tmp(std::vector<Value>(1, Value(TypeId::INTEGER, cnt)), &plan_->OutputSchema());
  *tuple = tmp;
  has_no_tuple_ = true;
  return true;
}
}  // namespace bustub
