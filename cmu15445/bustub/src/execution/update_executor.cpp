//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)),
      has_no_tuple_(false) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  // printf("%d update %d  \n", txn->GetTransactionId(), plan_->table_oid_);
  try {
    if (!lock_manager->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_)) {
      throw ExecutionException("delete lock table");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("delete lock table");
  }
  // printf("%d update get %d\n", txn->GetTransactionId(), plan_->table_oid_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto lock_manager = exec_ctx_->GetLockManager();
  auto txn = exec_ctx_->GetTransaction();
  Tuple update_tuple;
  RID update_rid;
  int cnt = 0;
  if (has_no_tuple_) {
    return false;
  }
  while (child_executor_->Next(&update_tuple, &update_rid)) {
    TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    try {
      if (!lock_manager->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, update_rid)) {
        throw ExecutionException("update lock row");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("update lock row");
    }
    Tuple old_tuple;
    if (!table_info->table_->GetTuple(update_rid, &old_tuple, txn)) {
      LOG_ERROR("can't get old tuple");
      exit(-1);
    }
    if (table_info->table_->UpdateTuple(update_tuple, update_rid, exec_ctx_->GetTransaction())) {
      cnt++;
      auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto info : vec) {
        auto old_key = old_tuple.KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
        info->index_->DeleteEntry(old_key, update_rid, exec_ctx_->GetTransaction());
        auto update_key =
            update_tuple.KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
        info->index_->InsertEntry(update_key, update_rid, exec_ctx_->GetTransaction());
        auto update_index_record = IndexWriteRecord(update_rid, plan_->table_oid_, WType::UPDATE, update_tuple,
                                                    info->index_oid_, exec_ctx_->GetCatalog());
        update_index_record.old_tuple_ = old_tuple;
        txn->AppendIndexWriteRecord(update_index_record);
      }
      // printf("txn{%d} update rid{%u}\n", txn->GetTransactionId(), update_rid.GetSlotNum());
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
