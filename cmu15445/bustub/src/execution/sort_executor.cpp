#include "execution/executors/sort_executor.h"
#include "algorithm"
namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    ans_.push_back(tuple);
  }
  std::sort(ans_.begin(), ans_.end(), [this](const Tuple &t1, const Tuple &t2) {
    for (auto [type, expr] : plan_->GetOrderBy()) {
      Value v1 = expr->Evaluate(&t1, child_executor_->GetOutputSchema());
      Value v2 = expr->Evaluate(&t2, child_executor_->GetOutputSchema());
      if (type == OrderByType::DEFAULT || type == OrderByType::ASC) {
        if (v1.CompareLessThan(v2) == CmpBool::CmpTrue) {
          return true;
        }
        if (v1.CompareGreaterThan(v2) == CmpBool::CmpTrue) {
          return false;
        }
      }
      if (type == OrderByType::DESC) {
        if (v1.CompareLessThan(v2) == CmpBool::CmpTrue) {
          return false;
        }
        if (v1.CompareGreaterThan(v2) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    // TO DO check
    return true;
  });

  std::reverse(ans_.begin(), ans_.end());
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ans_.empty()) {
    return false;
  }
  *tuple = ans_.back();
  *rid = tuple->GetRid();
  ans_.pop_back();
  return true;
}

}  // namespace bustub
