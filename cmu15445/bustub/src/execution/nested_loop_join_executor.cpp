//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <algorithm>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(left_executor)),
      right_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  while (left_executor_->Next(&tuple, &rid)) {
    left_.push_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_.push_back(tuple);
  }
  auto left_shema = left_executor_->GetOutputSchema();
  auto right_shema = right_executor_->GetOutputSchema();
  std::vector<Column> cols;
  for (const auto &col : left_shema.GetColumns()) {
    cols.push_back(col);
  }
  for (const auto &col : right_shema.GetColumns()) {
    cols.push_back(col);
  }
  Schema output(cols);
  if (plan_->GetJoinType() == JoinType::LEFT) {
    for (const auto &left : left_) {
      bool flag = false;
      for (const auto &right : right_) {
        if (plan_->predicate_
                ->EvaluateJoin(&left, left_executor_->GetOutputSchema(), &right, right_executor_->GetOutputSchema())
                .CompareEquals(Value(TypeId::BOOLEAN, static_cast<int8_t>(1))) == CmpBool::CmpTrue) {
          flag = true;
          std::vector<Value> val;
          for (uint32_t i = 0; i < left_shema.GetColumnCount(); i++) {
            val.push_back(left.GetValue(&left_shema, i));
          }
          for (uint32_t i = 0; i < right_shema.GetColumnCount(); i++) {
            val.push_back(right.GetValue(&right_shema, i));
          }
          ans_.emplace_back(val, &output);
        }
      }

      if (!flag) {
        std::vector<Value> val;
        for (uint32_t i = 0; i < left_shema.GetColumnCount(); i++) {
          val.push_back(left.GetValue(&left_shema, i));
        }
        for (uint32_t i = 0; i < right_shema.GetColumnCount(); i++) {
          val.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        }
        ans_.emplace_back(val, &output);
      }
    }
  } else if (plan_->GetJoinType() == JoinType::INNER) {
    for (const auto &left : left_) {
      for (const auto &right : right_) {
        if (plan_->predicate_
                ->EvaluateJoin(&left, left_executor_->GetOutputSchema(), &right, right_executor_->GetOutputSchema())
                .CompareEquals(Value(TypeId::BOOLEAN, static_cast<int8_t>(1))) == CmpBool::CmpTrue) {
          std::vector<Value> val;
          for (uint32_t i = 0; i < left_shema.GetColumnCount(); i++) {
            val.push_back(left.GetValue(&left_shema, i));
          }
          for (uint32_t i = 0; i < right_shema.GetColumnCount(); i++) {
            val.push_back(right.GetValue(&right_shema, i));
          }
          ans_.emplace_back(val, &output);
        }
      }
    }
  }
  std::reverse(ans_.begin(), ans_.end());
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!ans_.empty()) {
    *tuple = ans_.back();
    ans_.pop_back();
    return true;
  }
  return false;
}

}  // namespace bustub
