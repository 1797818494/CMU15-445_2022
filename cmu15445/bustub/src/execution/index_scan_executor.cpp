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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get())),
      begin_(tree_->GetBeginIterator()),
      end_(tree_->GetEndIterator()) {
  // tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn
  // *>(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get());
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (begin_ != end_) {
    *rid = (*begin_).second;
    std::string name = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->table_name_;
    exec_ctx_->GetCatalog()->GetTable(name)->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    ++begin_;
    return true;
  }
  return false;
}

}  // namespace bustub
