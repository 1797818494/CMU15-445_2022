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
  iter_begin_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->Begin(exec_ctx_->GetTransaction());
  iter_end_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_)->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_begin_ == iter_end_) {
    return false;
  }
  *tuple = *iter_begin_;
  *rid = tuple->GetRid();
  iter_begin_++;
  return true;
}

}  // namespace bustub
