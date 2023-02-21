#include "execution/executors/topn_executor.h"
#include <queue>
namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void TopNExecutor::Init() {
  cnt_ = plan_->n_;
  auto I = [this](const Tuple &t1, const Tuple &t2) -> bool {
    for (auto [type, expr] : plan_->GetOrderBy()) {
      Value v1 = expr->Evaluate(&t1, child_executor_->GetOutputSchema());
      Value v2 = expr->Evaluate(&t2, child_executor_->GetOutputSchema());
      if (type == OrderByType::DEFAULT || type == OrderByType::ASC) {
        if (v1.CompareLessThan(v2) == CmpBool::CmpTrue) {
          return false;
        }
        if (v1.CompareGreaterThan(v2) == CmpBool::CmpTrue) {
          return true;
        }
      }
      if (type == OrderByType::DESC) {
        if (v1.CompareLessThan(v2) == CmpBool::CmpTrue) {
          return true;
        }
        if (v1.CompareGreaterThan(v2) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    // TO DO check
    return true;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(I)> q(I);

  Tuple tuple;
  RID rid;
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    q.push(tuple);
  }
  while (cnt_-- > 0 && !q.empty()) {
    ans_.push(q.top());
    q.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ans_.empty()) {
    return false;
  }
  *tuple = ans_.front();
  *rid = (*tuple).GetRid();
  ans_.pop();
  return true;
}

}  // namespace bustub
