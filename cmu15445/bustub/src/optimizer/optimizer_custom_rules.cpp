#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"
#include "seq_scan_plan.h"
#include "filter_plan.h"
#include "index_scan_plan.h"
// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsIndexJoin(p);
  // p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  // my add
  p = OptimizeIndexScan(p);
  p = OptimizeMergeFilterScan(p);
  return p;
}

auto Optimizer::OptimizeIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (plan->GetType() == PlanType::Filter && plan->GetChildren().size() == 1 &&
      plan->GetChildAt(0)->GetType() == PlanType::SeqScan) {
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "index scan no possible !!!");
    const auto &child_plan = optimized_plan->children_[0];
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode&>(*child_plan);
    const auto &filter_plan = dynamic_cast<const FilterPlanNode&>(*plan);
    // to do judge the filter type
    return std::make_shared<IndexScanPlanNode>(plan->output_schema_, child_plan->GetChildAt(0), ,
                                          limit_plan.limit_);
  }
  return optimized_plan;
}

}  // namespace bustub
