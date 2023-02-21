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

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple delete_tuple;
  RID delete_rid;
  int cnt = 0;
  if (has_no_tuple_) {
    return false;
  }
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    if (table_info->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction())) {
      cnt++;
      auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto info : vec) {
        auto key = delete_tuple.KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
        info->index_->DeleteEntry(key, delete_rid, exec_ctx_->GetTransaction());
      }
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
