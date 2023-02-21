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

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple insert_tuple;
  RID insert_rid;
  int cnt = 0;
  if (has_no_tuple_) {
    return false;
  }
  while (child_executor_->Next(&insert_tuple, &insert_rid)) {
    TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    if (table_info->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction())) {
      cnt++;
      auto vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (auto info : vec) {
        auto key = insert_tuple.KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
        info->index_->InsertEntry(key, insert_rid, exec_ctx_->GetTransaction());
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
