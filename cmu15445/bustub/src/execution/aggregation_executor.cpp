//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::forward<std::unique_ptr<AbstractExecutor>>(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      has_no_value_(true) {}

void AggregationExecutor::Init() {
  Tuple aggregate_tuple;
  RID aggregate_rid;
  Value cmp(TypeId::BOOLEAN, static_cast<int8_t>(1));
  child_->Init();
  auto schema = child_->GetOutputSchema();
  while (child_->Next(&aggregate_tuple, &aggregate_rid)) {
    AggregateKey keys = MakeAggregateKey(&aggregate_tuple);
    AggregateValue values = MakeAggregateValue(&aggregate_tuple);
    aht_.InsertCombine(keys, values);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (has_no_value_ && plan_->GetGroupBys().empty()) {
      std::vector<Value> values(aht_.GenerateInitialAggregateValue().aggregates_);
      *tuple = Tuple(values, &plan_->OutputSchema());
      *rid = (*tuple).GetRid();
      has_no_value_ = false;
      return true;
    }
    return false;
  }
  auto key = aht_iterator_.Key().group_bys_;
  auto value = aht_iterator_.Val().aggregates_;
  auto result = key;
  for (const auto &item : value) {
    result.push_back(item);
  }
  *tuple = Tuple(result, plan_->output_schema_.get());
  *rid = tuple->GetRid();
  ++aht_iterator_;
  has_no_value_ = false;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
