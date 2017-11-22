/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport.task;

import com.carrotsearch.hppc.IntIntHashMap;
import io.crate.analyze.symbol.Assignments;
import io.crate.analyze.symbol.ParamSymbols;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.where.DocKeys;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.executor.JobTask;
import io.crate.executor.MultiActionListener;
import io.crate.executor.transport.OneRowActionListener;
import io.crate.executor.transport.ShardResponse;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.TransportShardUpsertAction;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.metadata.Functions;
import io.crate.operation.projectors.sharding.ShardingUpsertExecutor;
import io.crate.planner.node.dml.UpdateById;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.executor.transport.task.DeleteByIdTask.getRowCounts;
import static io.crate.executor.transport.task.DeleteByIdTask.getShardId;

public class UpdateByIdTask extends JobTask {

    private final ClusterService clusterService;
    private final Functions functions;
    private final TransportShardUpsertAction shardUpsertAction;
    private final UpdateById updateById;
    private final ShardUpsertRequest.Builder requestBuilder;
    private final Symbol[] assignmentSources;

    public UpdateByIdTask(ClusterService clusterService,
                          Functions functions,
                          TransportShardUpsertAction shardUpsertAction,
                          UpdateById updateById) {
        super(updateById.jobId());
        this.clusterService = clusterService;
        this.functions = functions;
        this.shardUpsertAction = shardUpsertAction;
        this.updateById = updateById;
        Tuple<String[], Symbol[]> assignments = Assignments.convert(updateById.assignmentByTargetCol());
        assignmentSources = assignments.v2();
        this.requestBuilder = new ShardUpsertRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(clusterService.state().metaData().settings()),
            false,
            false,
            assignments.v1(),
            null, // missing assignments are for INSERT .. ON DUPLICATE KEY UPDATE
            updateById.jobId(),
            // TODO: generated column validation is probably necessary - without values we can't do that in the analyzer?
            // we could do it after bind; so we could interrupt execution and avoid partial updates
            false
        );
    }

    // TODO: consider sharing more code with DeleteByIdTask

    @Override
    public void execute(RowConsumer consumer, Row parameters) {
        HashMap<ShardId, ShardUpsertRequest> requestsByShard = new HashMap<>();
        addRequests(0, parameters, requestsByShard);
        if (requestsByShard.isEmpty()) {
            consumer.accept(InMemoryBatchIterator.of(new Row1(0L), SENTINEL), null);
            return;
        }
        MultiActionListener<ShardResponse, long[], ? super Row> listener = new MultiActionListener<>(
            requestsByShard.size(),
            () -> new long[]{0},
            DeleteByIdTask::updateRowCountOrFail,
            rowCount -> new Row1(rowCount[0]),
            new OneRowActionListener<>(consumer, Function.identity())
        );
        for (ShardUpsertRequest request : requestsByShard.values()) {
            shardUpsertAction.execute(request, listener);
        }
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk(List<Row> bulkParams) {
        HashMap<ShardId, ShardUpsertRequest> requestsByShard = new HashMap<>();
        ArrayList<CompletableFuture<Long>> results = new ArrayList<>(bulkParams.size());
        int location = 0;
        IntIntHashMap resultIdxByLocation = new IntIntHashMap(bulkParams.size());
        for (int i = 0; i < bulkParams.size(); i++) {
            resultIdxByLocation.put(location, i);
            results.add(new CompletableFuture<>());
            location = addRequests(location, bulkParams.get(i), requestsByShard);
        }
        Collector<ShardResponse, ?, List<ShardResponse>> shardResponseListCollector = Collectors.toList();
        MultiActionListener<ShardResponse, ?, List<ShardResponse>> listener =
            new MultiActionListener<>(requestsByShard.size(), shardResponseListCollector, new ActionListener<List<ShardResponse>>() {

                @Override
                public void onResponse(List<ShardResponse> responses) {
                    long[] rowCounts = getRowCounts(responses, bulkParams, resultIdxByLocation);
                    for (int i = 0; i < results.size(); i++) {
                        results.get(i).complete(rowCounts[i]);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    results.forEach(s -> s.completeExceptionally(e));
                }
            });
        for (ShardUpsertRequest request : requestsByShard.values()) {
            shardUpsertAction.execute(request, listener);
        }
        return results;
    }

    private int addRequests(int location, Row parameters, HashMap<ShardId, ShardUpsertRequest> requestsByShard) {
        Symbol[] assignments = bindAssignments(assignmentSources, parameters);
        for (DocKeys.DocKey docKey : updateById.docKeys()) {
            String id = docKey.getId(functions, parameters);
            if (id == null) {
                continue;
            }
            String routing = docKey.getRouting(functions, parameters);
            List<BytesRef> partitionValues = docKey.getPartitionValues(functions, parameters);
            String indexName = ESGetTask.indexName(updateById.table(), partitionValues);
            ShardId shardId;
            try {
                shardId = getShardId(clusterService, indexName, id, routing);
            } catch (IndexNotFoundException e) {
                if (updateById.table().isPartitioned()) {
                    continue;
                }
                throw e;
            }
            ShardUpsertRequest request = requestsByShard.get(shardId);
            if (request == null) {
                request = requestBuilder.newRequest(shardId, routing);
                requestsByShard.put(shardId, request);
            }
            Long version = docKey.version(functions, parameters).orElse(null);
            ShardUpsertRequest.Item item = new ShardUpsertRequest.Item(id, assignments, null, version);
            request.add(location, item);
            location++;
        }
        return location;
    }

    public static Symbol[] bindAssignments(Symbol[] assignmentSources, Row parameters) {
        Symbol[] assignments = new Symbol[assignmentSources.length];
        for (int i = 0; i < assignmentSources.length; i++) {
            assignments[i] = ParamSymbols.toLiterals(assignmentSources[i], parameters);
        }
        return assignments;
    }
}
