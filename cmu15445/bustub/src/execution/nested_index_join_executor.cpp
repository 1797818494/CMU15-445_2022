//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <algorithm>
#include "type/value_factory.h"
namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_);
  auto right_shema = *plan_->inner_table_schema_;
  auto left_shema = child_executor_->GetOutputSchema();
  std::vector<Column> cols;
  auto left_cols = left_shema.GetColumns();
  auto right_cols = right_shema.GetColumns();
  cols.reserve(left_cols.size() + right_cols.size());
  for (const auto &col : left_cols) {
    cols.push_back(col);
  }
  for (const auto &col : right_cols) {
    cols.push_back(col);
  }
  Schema output_schema(cols);
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<RID> result_rid;
    std::vector<Value> vals1;
    auto key = plan_->KeyPredicate()->Evaluate(&tuple, table_info->schema_);
    vals1.push_back(key);
    Tuple left(vals1, index_info->index_->GetKeySchema());
    index_info->index_->ScanKey(left, &result_rid, exec_ctx_->GetTransaction());
    // TO DO RID find Tuple ...
    Tuple right_tuple;
    bool flag = true;
    for (auto result : result_rid) {
      flag = false;
      table_info->table_->GetTuple(result, &right_tuple, exec_ctx_->GetTransaction());
      std::vector<Value> vals;
      for (uint32_t i = 0; i < left_shema.GetColumnCount(); i++) {
        vals.push_back(tuple.GetValue(&left_shema, i));
      }
      for (uint32_t i = 0; i < right_shema.GetColumnCount(); i++) {
        vals.push_back(right_tuple.GetValue(&right_shema, i));
      }
      ans_.emplace_back(vals, &output_schema);
    }
    if (plan_->GetJoinType() == JoinType::LEFT && flag) {
      std::vector<Value> vals;
      for (uint32_t i = 0; i < left_shema.GetColumnCount(); i++) {
        vals.push_back(tuple.GetValue(&left_shema, i));
      }
      for (uint32_t i = 0; i < right_shema.GetColumnCount(); i++) {
        vals.push_back(ValueFactory::GetNullValueByType(right_shema.GetColumn(i).GetType()));
      }
      ans_.emplace_back(vals, &output_schema);
    }
  }
  std::reverse(ans_.begin(), ans_.end());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ans_.empty()) {
    return false;
  }
  *tuple = ans_.back();
  *rid = (*tuple).GetRid();
  ans_.pop_back();
  return true;
}

}  // namespace bustub
