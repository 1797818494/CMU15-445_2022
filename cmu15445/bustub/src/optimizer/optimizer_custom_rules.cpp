#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"
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
    children.emplace_back(OptimizeIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // LOG_INFO("get optimized type{%d}  childtype{%d}", (int)optimized_plan->GetType(),
  // (int)optimized_plan->GetChildAt(0)->GetType());
  if (optimized_plan->GetType() == PlanType::Filter && optimized_plan->GetChildAt(0)->GetType() == PlanType::SeqScan) {
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "index scan no possible !!!");
    // const auto &child_plan = optimized_plan->children_[0];
    // const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*child_plan);
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan->GetChildAt(0));
    // to do judge the filter type
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(filter_plan.predicate_.get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            if (auto index = MatchIndex(seq_plan.table_name_, left_expr->GetColIdx()); index != std::nullopt) {
              auto [index_oid, index_name] = *index;
              auto index_plan = std::make_shared<IndexScanPlanNode>(plan->output_schema_, index_oid);
              index_plan->val_ = right_expr->val_;
              index_plan->table_name_ = seq_plan.table_name_;
              // LOG_INFO("get here");
              return index_plan;
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}
}  // namespace bustub
