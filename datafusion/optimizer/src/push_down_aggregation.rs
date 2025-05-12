// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Push Down Aggregation optimizer rule ensures that aggregations are applied as early as possible in the plan

use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DataFusionError, Result};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::logical_plan::TableScanAggregate;
use datafusion_expr::logical_plan::{LogicalPlan, TableScan};
use datafusion_expr::utils::grouping_set_to_exprlist;
use datafusion_expr::{col, Aggregate, AggregateUDF, LogicalPlanBuilder};
use datafusion_expr::{Expr, TableProviderAggregationPushDown};
use std::ops::Deref;
use std::sync::Arc;
use datafusion_functions_aggregate::sum::Sum;

#[derive(Default, Debug)]
pub struct PushDownAggregation {}

impl PushDownAggregation {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownAggregation {
    fn name(&self) -> &str {
        "push_down_aggregation"
    }

    fn rewrite(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
            ..
        }) = plan
        {
            if !check_push_down_support(aggr_expr) {
                return Ok(None);
            }

            if let LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                aggregate,
                fetch,
                ..
            }) = input.as_ref()
            {
                if aggregate.is_none() {
                    let new_plan = match source
                        .supports_aggregate_pushdown(group_expr, aggr_expr)?
                    {
                        TableProviderAggregationPushDown::Unsupported => None,
                        TableProviderAggregationPushDown::Ungrouped => {
                            let new_aggr_expr = aggr_expr.iter().map(|e| {
                                let col_name = e.to_string();
                                let col_expr = col(&col_name);
                                let new_expr = match e {
                                    Expr::AggregateFunction(AggregateFunction {
                                        func,
                                        params:
                                            AggregateFunctionParams {
                                                args,
                                                distinct,
                                                filter,
                                                order_by,
                                                null_treatment,
                                            },
                                    }) => {
                                        let new_aggr_func = match func.name() {
                                            "min" | "max" | "sum" => {
                                                AggregateFunction {
                                                    func: func.clone(),
                                                    params: AggregateFunctionParams {
                                                        args: vec![col_expr],
                                                        distinct: *distinct,
                                                        filter: filter.clone(),
                                                        order_by: order_by.clone(),
                                                        null_treatment: null_treatment.clone(),
                                                    }
                                                }
                                            },
                                            "count" => {
                                                AggregateFunction {
                                                    func: Arc::new(AggregateUDF::new_from_impl(Sum::new())),
                                                    params: AggregateFunctionParams {
                                                        args: vec![col_expr],
                                                        distinct: *distinct,
                                                        filter: filter.clone(),
                                                        order_by: order_by.clone(),
                                                        null_treatment: null_treatment.clone(),
                                                    }
                                                }
                                            },
                                            _ => {
                                                return Err(DataFusionError::Internal(format!("Unreachable, not support {func:?}")));
                                            }
                                        };
                                        Ok(Expr::AggregateFunction(new_aggr_func))
                                    }
                                    other => {
                                        Err(DataFusionError::Internal(format!(
                                            "Invalid LogicalPlan, Aggregate::aggr_expr contains non-aggregatable expr: {other:?}"
                                        )))
                                    }
                                }?;

                                let alias = col(new_expr.name_for_alias()?).alias(col_name);
                                Ok((new_expr, alias))
                            }).collect::<Result<Vec<_>>>()?;

                            let (new_aggr_expr, projection_aggr_expr) = new_aggr_expr.into_iter().unzip();

                            let new_required_col = ();
                            let all_group_expr = grouping_set_to_exprlist(group_expr)?;
                            exprlist

                        }
                        TableProviderAggregationPushDown::Grouped => {
                            // Remove `Aggregate` node
                            // Change the optimized logical plan to reflect the pushed down aggregate
                            //
                            // e.g.
                            //
                            // Aggregate: groupBy=[[]], aggr=[[min(c1), max(c1)]]
                            //   TableScan: t1 projection=[c1]
                            // ->
                            // == Optimized Logical Plan ==
                            // TableScan: t1 projection=[c1] groupBy=[[]], aggr=[[min(c1), max(c1)]]
                            Some(LogicalPlan::TableScan(TableScan {
                                table_name: table_name.clone(),
                                source: source.clone(),
                                projection: None,
                                projected_schema: schema.clone(),
                                filters: filters.clone(),
                                fetch: *fetch,
                                aggregate: Some(TableScanAggregate {
                                    group_expr: group_expr.clone(),
                                    aggr_expr: aggr_expr.clone(),
                                    schema: schema.clone(),
                                }),
                            }))
                        }
                    };
                }
            }
        }
        Ok(())
    }
}

fn check_push_down_support(aggr_expr: &[Expr]) -> bool {
    aggr_expr.iter().all(|e| match e {
        Expr::AggregateFunction(AggregateFunction {
            func,
            params: AggregateFunctionParams { distinct, .. },
            ..
        }) => matches!(func.name(), "max" | "min" | "sum" | "count") && !distinct,
        _ => false,
    })
}
