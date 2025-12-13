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
package io.trino.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Build operator for sort-merge ASOF join. Accumulates sorted pages from the
 * build side (right side) and provides them to probe operators via the bridge.
 */
public class SortMergeAsofJoinBuildOperator
        implements Operator
{
    public static class SortMergeAsofJoinBuildOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<SortMergeAsofJoinBridge> joinBridgeManager;
        private boolean closed;

        public SortMergeAsofJoinBuildOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<SortMergeAsofJoinBridge> joinBridgeManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridgeManager = requireNonNull(joinBridgeManager, "joinBridgeManager is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(
                    operatorId,
                    planNodeId,
                    SortMergeAsofJoinBuildOperator.class.getSimpleName());
            return new SortMergeAsofJoinBuildOperator(operatorContext, joinBridgeManager.getJoinBridge());
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SortMergeAsofJoinBuildOperatorFactory(operatorId, planNodeId, joinBridgeManager);
        }
    }

    private final OperatorContext operatorContext;
    private final SortMergeAsofJoinBridge joinBridge;
    private final SortMergeAsofJoinPagesBuilder pagesBuilder;
    private final LocalMemoryContext localUserMemoryContext;

    // Initially, probeDoneWithPages is not present.
    // Once finish is called, probeDoneWithPages will be set to a future that completes when the pages are no longer needed by the probe side.
    // When the pages are no longer needed, the isFinished method on this operator will return true.
    private Optional<ListenableFuture<Void>> probeDoneWithPages = Optional.empty();

    public SortMergeAsofJoinBuildOperator(
            OperatorContext operatorContext,
            SortMergeAsofJoinBridge joinBridge)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.joinBridge = requireNonNull(joinBridge, "joinBridge is null");
        this.pagesBuilder = new SortMergeAsofJoinPagesBuilder(operatorContext);
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (probeDoneWithPages.isPresent()) {
            return;
        }

        // pagesBuilder and the built SortMergeAsofJoinPages will mostly share the same objects.
        // Extra allocation is minimal during build call. As a result, memory accounting is not updated here.
        SortMergeAsofJoinPages pages = pagesBuilder.build();

        // Only set pages if we have actual data. This prevents "empty" build operators
        // from winning the race and setting the future to empty pages when other operators
        // might have data. If all operators are empty, probe operators will get empty results
        // which is correct for an ASOF join with no matching build rows.
        if (!pages.getPages().isEmpty()) {
            probeDoneWithPages = Optional.of(joinBridge.setPages(pages));
        }
        else {
            // Mark as finished without setting pages - we had no data to contribute
            probeDoneWithPages = Optional.of(joinBridge.whenBuildFinishes());
        }
    }

    @Override
    public boolean isFinished()
    {
        return probeDoneWithPages.map(Future::isDone).orElse(false);
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return probeDoneWithPages.orElse(NOT_BLOCKED);
    }

    @Override
    public boolean needsInput()
    {
        return probeDoneWithPages.isEmpty();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (page.getPositionCount() == 0) {
            return;
        }

        pagesBuilder.addPage(page);
        if (!localUserMemoryContext.trySetBytes(pagesBuilder.getEstimatedSize().toBytes())) {
            pagesBuilder.compact();
            localUserMemoryContext.setBytes(pagesBuilder.getEstimatedSize().toBytes());
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
