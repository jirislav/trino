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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.SystemSessionProperties;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.SortMergeAsofJoinBridge;
import io.trino.operator.join.SortMergeAsofJoinPages;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Comparison;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

/**
 * Sort-merge ASOF join operator (probe side).
 * Waits for build side (right) pages via bridge, then processes probe (left) pages.
 * Assumes both inputs are pre-sorted by equi-keys + ordering column.
 * For each probe row, finds the build row with matching equi-keys and
 * the closest ordering value satisfying the inequality condition.
 */
public class SortMergeAsofJoinOperator
        implements Operator
{
    public static class SortMergeAsofJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<SortMergeAsofJoinBridge> joinBridgeManager;
        private final List<Type> probeTypes;
        private final List<Type> buildTypes;
        private final List<Integer> probeEquiChannels;
        private final List<Integer> buildEquiChannels;
        private final int probeOrderingChannel;
        private final int buildOrderingChannel;
        private final Comparison.Operator inequalityOperator;
        private final BlockTypeOperators blockTypeOperators;
        private boolean closed;

        public SortMergeAsofJoinOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<SortMergeAsofJoinBridge> joinBridgeManager,
                List<Type> probeTypes,
                List<Type> buildTypes,
                List<Integer> probeEquiChannels,
                List<Integer> buildEquiChannels,
                int probeOrderingChannel,
                int buildOrderingChannel,
                Comparison.Operator inequalityOperator,
                BlockTypeOperators blockTypeOperators)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridgeManager = requireNonNull(joinBridgeManager, "joinBridgeManager is null");
            this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
            this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
            this.probeEquiChannels = ImmutableList.copyOf(requireNonNull(probeEquiChannels, "probeEquiChannels is null"));
            this.buildEquiChannels = ImmutableList.copyOf(requireNonNull(buildEquiChannels, "buildEquiChannels is null"));
            this.probeOrderingChannel = probeOrderingChannel;
            this.buildOrderingChannel = buildOrderingChannel;
            this.inequalityOperator = requireNonNull(inequalityOperator, "inequalityOperator is null");
            this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(
                    operatorId,
                    planNodeId,
                    SortMergeAsofJoinOperator.class.getSimpleName());

            // Get optimization flags from session
            boolean usePositionTracking = SystemSessionProperties.isAsofJoinUsePositionTrackingEnabled(driverContext.getSession());
            boolean useBinarySearch = SystemSessionProperties.isAsofJoinUseBinarySearchEnabled(driverContext.getSession());
            boolean useEarlyTermination = SystemSessionProperties.isAsofJoinEarlyTerminationEnabled(driverContext.getSession());

            return new SortMergeAsofJoinOperator(
                    operatorContext,
                    joinBridgeManager.getJoinBridge(),
                    probeTypes,
                    buildTypes,
                    probeEquiChannels,
                    buildEquiChannels,
                    probeOrderingChannel,
                    buildOrderingChannel,
                    inequalityOperator,
                    blockTypeOperators,
                    usePositionTracking,
                    useBinarySearch,
                    useEarlyTermination);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SortMergeAsofJoinOperatorFactory(
                    operatorId,
                    planNodeId,
                    joinBridgeManager,
                    probeTypes,
                    buildTypes,
                    probeEquiChannels,
                    buildEquiChannels,
                    probeOrderingChannel,
                    buildOrderingChannel,
                    inequalityOperator,
                    blockTypeOperators);
        }
    }

    private final OperatorContext operatorContext;
    private final ListenableFuture<SortMergeAsofJoinPages> buildPagesFuture;
    private final ListenableFuture<Void> blockedFutureView;
    private final List<Type> probeTypes;
    private final List<Type> buildTypes;
    private final List<Type> outputTypes;
    private final List<Integer> probeEquiChannels;
    private final List<Integer> buildEquiChannels;
    private final int probeOrderingChannel;
    private final int buildOrderingChannel;
    private final Comparison.Operator inequalityOperator;
    private final BlockTypeOperators blockTypeOperators;
    private final PageBuilder pageBuilder;

    // Optimization flags
    private final boolean usePositionTracking;
    private final boolean useBinarySearch;
    private final boolean useEarlyTermination;

    // Build side pages (buffered from bridge)
    private List<Page> buildPages;

    // Probe side state
    private Page currentProbePage;
    private int probePosition;
    private boolean finishing;

    // Position tracking state (for O(n+m) optimization)
    private int lastBuildPageIndex;
    private int lastBuildPosition;

    private SortMergeAsofJoinOperator(
            OperatorContext operatorContext,
            SortMergeAsofJoinBridge joinBridge,
            List<Type> probeTypes,
            List<Type> buildTypes,
            List<Integer> probeEquiChannels,
            List<Integer> buildEquiChannels,
            int probeOrderingChannel,
            int buildOrderingChannel,
            Comparison.Operator inequalityOperator,
            BlockTypeOperators blockTypeOperators,
            boolean usePositionTracking,
            boolean useBinarySearch,
            boolean useEarlyTermination)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.buildPagesFuture = joinBridge.getPagesFuture();
        this.blockedFutureView = transform(buildPagesFuture, _ -> null, directExecutor());
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
        this.outputTypes = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(buildTypes)
                .build();
        this.probeEquiChannels = ImmutableList.copyOf(requireNonNull(probeEquiChannels, "probeEquiChannels is null"));
        this.buildEquiChannels = ImmutableList.copyOf(requireNonNull(buildEquiChannels, "buildEquiChannels is null"));
        this.probeOrderingChannel = probeOrderingChannel;
        this.buildOrderingChannel = buildOrderingChannel;
        this.inequalityOperator = requireNonNull(inequalityOperator, "inequalityOperator is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        this.usePositionTracking = usePositionTracking;
        this.useBinarySearch = useBinarySearch;
        this.useEarlyTermination = useEarlyTermination;
        this.pageBuilder = new PageBuilder(outputTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && currentProbePage == null && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return blockedFutureView;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || currentProbePage != null) {
            return false;
        }

        // Wait for build pages to be ready
        if (buildPages == null) {
            Optional<SortMergeAsofJoinPages> pages = tryGetFutureValue(buildPagesFuture);
            if (pages.isPresent()) {
                buildPages = pages.get().getPages();
            }
        }

        return buildPages != null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(buildPages != null, "Build pages not ready yet");
        checkState(currentProbePage == null, "Current probe page has not been completely processed yet");

        if (page.getPositionCount() > 0) {
            currentProbePage = page;
            probePosition = 0;
        }
    }

    @Override
    public Page getOutput()
    {
        // Wait for build pages
        if (buildPages == null) {
            return null;
        }

        // Process current probe page
        if (currentProbePage != null) {
            processProbePage();
        }

        if (pageBuilder.isEmpty()) {
            return null;
        }

        Page output = pageBuilder.build();
        pageBuilder.reset();
        return output;
    }

    private void processProbePage()
    {
        while (probePosition < currentProbePage.getPositionCount()) {
            if (pageBuilder.isFull()) {
                return;
            }

            // Find matching build row for current probe row
            int matchingBuildPage = -1;
            int matchingBuildPosition = -1;

            if (usePositionTracking || useBinarySearch) {
                // Optimized path: use position tracking and/or binary search
                // Start from the last position (position tracking optimization)
                int startPageIndex = usePositionTracking ? lastBuildPageIndex : 0;
                int startPosition = usePositionTracking ? lastBuildPosition : 0;

                for (int buildPageIndex = startPageIndex; buildPageIndex < buildPages.size(); buildPageIndex++) {
                    Page buildPage = buildPages.get(buildPageIndex);
                    int startPos = (buildPageIndex == startPageIndex) ? startPosition : 0;

                    if (useBinarySearch && probeEquiChannels.isEmpty()) {
                        // Binary search for ordering column match (when no equi-keys)
                        int position = binarySearchBuildSide(buildPage, startPos, buildPage.getPositionCount());
                        if (position >= 0 && inequalitySatisfied(currentProbePage, probePosition, buildPage, position)) {
                            matchingBuildPage = buildPageIndex;
                            matchingBuildPosition = position;
                            // Update position tracking
                            if (usePositionTracking) {
                                lastBuildPageIndex = buildPageIndex;
                                lastBuildPosition = position;
                            }
                            break; // Found best match with binary search
                        }
                    }
                    else {
                        // Linear scan with early termination
                        for (int buildPosition = startPos; buildPosition < buildPage.getPositionCount(); buildPosition++) {
                            // Check if equi-keys match
                            if (!equiKeysMatch(currentProbePage, probePosition, buildPage, buildPosition)) {
                                // Early termination: if equi-keys don't match and data is sorted by equi-keys,
                                // we can skip ahead until we find matching equi-keys or determine no match exists
                                if (useEarlyTermination && !probeEquiChannels.isEmpty()) {
                                    // Skip to next equi-key group
                                    int cmp = compareEquiKeys(currentProbePage, probePosition, buildPage, buildPosition);
                                    if (cmp < 0) {
                                        // Probe key < build key: no more matches in this or future pages
                                        buildPageIndex = buildPages.size(); // Exit outer loop
                                        break;
                                    }
                                }
                                continue;
                            }

                            // Check if inequality condition is satisfied
                            if (!inequalitySatisfied(currentProbePage, probePosition, buildPage, buildPosition)) {
                                continue;
                            }

                            // This is a candidate match. Check if it's better than current best.
                            if (matchingBuildPage == -1 ||
                                    isBetterMatch(buildPage, buildPosition, buildPages.get(matchingBuildPage), matchingBuildPosition)) {
                                matchingBuildPage = buildPageIndex;
                                matchingBuildPosition = buildPosition;

                                // Update position tracking
                                if (usePositionTracking) {
                                    lastBuildPageIndex = buildPageIndex;
                                    lastBuildPosition = buildPosition;
                                }
                            }
                        }
                    }
                }
            }
            else {
                // Original unoptimized path: scan all build pages
                for (int buildPageIndex = 0; buildPageIndex < buildPages.size(); buildPageIndex++) {
                    Page buildPage = buildPages.get(buildPageIndex);

                    for (int buildPosition = 0; buildPosition < buildPage.getPositionCount(); buildPosition++) {
                        // Check if equi-keys match
                        if (!equiKeysMatch(currentProbePage, probePosition, buildPage, buildPosition)) {
                            continue;
                        }

                        // Check if inequality condition is satisfied
                        if (!inequalitySatisfied(currentProbePage, probePosition, buildPage, buildPosition)) {
                            continue;
                        }

                        // This is a candidate match. Check if it's better than current best.
                        if (matchingBuildPage == -1 ||
                                isBetterMatch(buildPage, buildPosition, buildPages.get(matchingBuildPage), matchingBuildPosition)) {
                            matchingBuildPage = buildPageIndex;
                            matchingBuildPosition = buildPosition;
                        }
                    }
                }
            }

            // Output the join result
            pageBuilder.declarePosition();

            // Append probe columns
            for (int channel = 0; channel < probeTypes.size(); channel++) {
                Block probeBlock = currentProbePage.getBlock(channel);
                pageBuilder.getBlockBuilder(channel).append(probeBlock.getUnderlyingValueBlock(), probeBlock.getUnderlyingValuePosition(probePosition));
            }

            // Append build columns (or nulls if no match)
            for (int channel = 0; channel < buildTypes.size(); channel++) {
                if (matchingBuildPage != -1) {
                    Block buildBlock = buildPages.get(matchingBuildPage).getBlock(channel);
                    pageBuilder.getBlockBuilder(probeTypes.size() + channel).append(buildBlock.getUnderlyingValueBlock(), buildBlock.getUnderlyingValuePosition(matchingBuildPosition));
                }
                else {
                    pageBuilder.getBlockBuilder(probeTypes.size() + channel).appendNull();
                }
            }

            probePosition++;
        }

        // Finished processing current probe page
        currentProbePage = null;
        probePosition = 0;
        // Reset position tracking for next probe page
        lastBuildPageIndex = 0;
        lastBuildPosition = 0;
    }

    private boolean equiKeysMatch(Page probePage, int probePosition, Page buildPage, int buildPosition)
    {
        for (int i = 0; i < probeEquiChannels.size(); i++) {
            int probeChannel = probeEquiChannels.get(i);
            int buildChannel = buildEquiChannels.get(i);

            Block probeBlock = probePage.getBlock(probeChannel);
            Block buildBlock = buildPage.getBlock(buildChannel);

            Type type = probeTypes.get(probeChannel);
            BlockPositionComparison comparison = blockTypeOperators.getComparisonUnorderedLastOperator(type);

            if (comparison.compare(probeBlock, probePosition, buildBlock, buildPosition) != 0) {
                return false;
            }
        }
        return true;
    }

    private boolean inequalitySatisfied(Page probePage, int probePosition, Page buildPage, int buildPosition)
    {
        Block probeBlock = probePage.getBlock(probeOrderingChannel);
        Block buildBlock = buildPage.getBlock(buildOrderingChannel);

        Type orderingType = probeTypes.get(probeOrderingChannel);
        BlockPositionComparison comparison = blockTypeOperators.getComparisonUnorderedLastOperator(orderingType);

        // Compare build vs probe
        // The inequality operator comes from the original query and defines the relationship
        long cmp = comparison.compare(buildBlock, buildPosition, probeBlock, probePosition);

        return switch (inequalityOperator) {
            // For LESS_THAN and LESS_THAN_OR_EQUAL, the original query had:
            // - "r.ts < l.ts" or "l.ts > r.ts": want build < probe
            // - "r.ts <= l.ts" or "l.ts >= r.ts": want build <= probe
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUAL -> cmp <= 0;
            // For GREATER_THAN and GREATER_THAN_OR_EQUAL, the original query had:
            // - "r.ts > l.ts" or "l.ts < r.ts": want build > probe
            // - "r.ts >= l.ts" or "l.ts <= r.ts": want build >= probe
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUAL -> cmp >= 0;
            default -> false;
        };
    }

    private boolean isBetterMatch(Page candidatePage, int candidatePosition, Page currentBestPage, int currentBestPosition)
    {
        Block candidateBlock = candidatePage.getBlock(buildOrderingChannel);
        Block currentBestBlock = currentBestPage.getBlock(buildOrderingChannel);

        Type orderingType = buildTypes.get(buildOrderingChannel);
        BlockPositionComparison comparison = blockTypeOperators.getComparisonUnorderedLastOperator(orderingType);

        long cmp = comparison.compare(candidateBlock, candidatePosition, currentBestBlock, currentBestPosition);

        // For ASOF join, we want the "closest" match:
        // - For <= and <: want MAXIMUM build value (closest from below) - prefer larger
        // - For >= and >: want MINIMUM build value (closest from above) - prefer smaller
        return switch (inequalityOperator) {
            case LESS_THAN, LESS_THAN_OR_EQUAL -> cmp > 0;  // prefer larger (closer to probe from below)
            case GREATER_THAN, GREATER_THAN_OR_EQUAL -> cmp < 0;  // prefer smaller (closer to probe from above)
            default -> false;
        };
    }

    /**
     * Binary search for the best matching position in the build side.
     * Returns the position of the closest match, or -1 if no match found.
     */
    private int binarySearchBuildSide(Page buildPage, int fromIndex, int toIndex)
    {
        Block probeBlock = currentProbePage.getBlock(probeOrderingChannel);
        Block buildBlock = buildPage.getBlock(buildOrderingChannel);
        Type orderingType = probeTypes.get(probeOrderingChannel);
        BlockPositionComparison comparison = blockTypeOperators.getComparisonUnorderedLastOperator(orderingType);

        int low = fromIndex;
        int high = toIndex - 1;
        int bestMatch = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long cmp = comparison.compare(buildBlock, mid, probeBlock, probePosition);

            if (cmp == 0) {
                return mid; // Exact match
            }
            else if (cmp < 0) {
                // build < probe
                if (inequalityOperator == LESS_THAN || inequalityOperator == LESS_THAN_OR_EQUAL) {
                    bestMatch = mid; // This could be a match
                }
                low = mid + 1;
            }
            else {
                // build > probe
                if (inequalityOperator == GREATER_THAN || inequalityOperator == GREATER_THAN_OR_EQUAL) {
                    bestMatch = mid; // This could be a match
                }
                high = mid - 1;
            }
        }

        return bestMatch;
    }

    /**
     * Compare equi-keys between probe and build rows.
     * Returns negative if probe < build, 0 if equal, positive if probe > build.
     */
    private int compareEquiKeys(Page probePage, int probePosition, Page buildPage, int buildPosition)
    {
        for (int i = 0; i < probeEquiChannels.size(); i++) {
            int probeChannel = probeEquiChannels.get(i);
            int buildChannel = buildEquiChannels.get(i);

            Block probeBlock = probePage.getBlock(probeChannel);
            Block buildBlock = buildPage.getBlock(buildChannel);

            Type type = probeTypes.get(probeChannel);
            BlockPositionComparison comparison = blockTypeOperators.getComparisonUnorderedLastOperator(type);

            long cmp = comparison.compare(probeBlock, probePosition, buildBlock, buildPosition);
            if (cmp != 0) {
                return (int) cmp;
            }
        }
        return 0;
    }
}
