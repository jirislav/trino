/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner.iterative.rule;

import io.airlift.units.DataSize;
import io.trino.SystemSessionProperties;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.OptimizerConfig.AsofJoinStrategy;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;

import static io.trino.sql.planner.plan.Patterns.join;

/**
 * Cost-based rule to choose between windowing and sort-merge ASOF join strategies.
 * <p>
 * Decision factors:
 * - Build side row count estimate
 * - Presence of equi-keys
 * - Data distribution
 * <p>
 * Strategy selection:
 * - Windowing: Better for small build sides (configurable threshold, default 1M rows)
 * - Sort-merge: Better for large build sides or when windowing can't be used
 */
public class ChooseAsofJoinStrategy
        implements Rule<JoinNode>
{
    private final RewriteAsofJoinToLeftJoinWithTop1 windowingRule;
    private final RewriteAsofJoinToSortMergeAsofJoin sortMergeRule;

    public ChooseAsofJoinStrategy(PlannerContext plannerContext)
    {
        this.windowingRule = new RewriteAsofJoinToLeftJoinWithTop1(plannerContext);
        this.sortMergeRule = new RewriteAsofJoinToSortMergeAsofJoin();
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return join().matching(node -> node.getType() == JoinType.ASOF);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        // Check if user has explicitly set a strategy
        AsofJoinStrategy strategy = SystemSessionProperties.getAsofJoinStrategy(context.getSession());

        switch (strategy) {
            case WINDOWING:
                // Force windowing strategy
                return windowingRule.apply(node, captures, context);

            case SORT_MERGE:
                // Force sort-merge strategy
                return sortMergeRule.apply(node, captures, context);

            case AUTOMATIC:
            default:
                // Use cost-based selection
                // Get statistics for build side (right side)
                StatsProvider statsProvider = context.getStatsProvider();
                PlanNodeStatsEstimate rightStats = statsProvider.getStats(node.getRight());

                // Choose strategy based on build side size
                boolean useWindowing = shouldUseWindowing(rightStats, node, context);

                if (useWindowing) {
                    // Try windowing approach first (better for small build sides)
                    // Fall back to sort-merge if windowing doesn't support the query pattern
                    Result windowingResult;
                    try {
                        windowingResult = windowingRule.apply(node, captures, context);
                    }
                    catch (Exception e) {
                        // Windowing rule threw an exception (e.g., unsupported inequality direction)
                        // Fall back to sort-merge which supports all inequality patterns
                        return sortMergeRule.apply(node, captures, context);
                    }
                    if (windowingResult.isEmpty()) {
                        // Windowing rule couldn't handle this query, fall back to sort-merge
                        return sortMergeRule.apply(node, captures, context);
                    }
                    return windowingResult;
                }
                else {
                    // Use sort-merge approach (better for large build sides)
                    return sortMergeRule.apply(node, captures, context);
                }
        }
    }

    private boolean shouldUseWindowing(PlanNodeStatsEstimate buildStats, JoinNode node, Context context)
    {
        // If we have no statistics, prefer sort-merge (more scalable)
        if (buildStats.isOutputRowCountUnknown()) {
            return false;
        }

        // Get configurable thresholds from session properties
        long thresholdRows = SystemSessionProperties.getAsofJoinWindowingThresholdRows(context.getSession());
        DataSize thresholdSize = SystemSessionProperties.getAsofJoinWindowingThresholdSize(context.getSession());

        double buildRowCount = buildStats.getOutputRowCount();

        // Windowing is better for small build sides
        if (buildRowCount < thresholdRows) {
            return true;
        }

        // For medium-sized builds, consider data size estimate
        double buildSizeBytes = buildStats.getOutputSizeInBytes(node.getRight().getOutputSymbols());
        if (!Double.isNaN(buildSizeBytes) && buildSizeBytes < thresholdSize.toBytes()) {
            return true;
        }

        // Default to sort-merge for large build sides
        return false;
    }
}
