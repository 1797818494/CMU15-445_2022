#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (plan->GetType() == PlanType::Limit && plan->GetChildren().size() == 1 &&
      plan->GetChildAt(0)->GetType() == PlanType::Sort) {
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "sort limt no possible !!!");
    const auto &child_plan = optimized_plan->children_[0];
    const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*plan);
    return std::make_shared<TopNPlanNode>(plan->output_schema_, child_plan->GetChildAt(0), sort_plan.order_bys_,
                                          limit_plan.limit_);
  }
  return optimized_plan;
}

}  // namespace bustub
