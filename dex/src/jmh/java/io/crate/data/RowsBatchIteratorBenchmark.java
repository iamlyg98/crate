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

package io.crate.data;

import io.crate.data.join.CombinedRow;
import io.crate.data.join.NestedLoopBatchIterator;
import io.crate.testing.RowGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static io.crate.data.SentinelRow.SENTINEL;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RowsBatchIteratorBenchmark {

    // use materialize to not have shared row instances
    // this is done in the startup, otherwise the allocation costs will make up the majority of the benchmark.
    private List<Row> rows = StreamSupport.stream(RowGenerator.range(0, 10_000_000).spliterator(), false)
        .map(Row::materialize)
        .map(RowN::new)
        .collect(Collectors.toList());

    // used with  RowsBatchIterator without any state/error handling to establish a performance baseline.
    private final List<Row1> oneThousandRows = IntStream.range(0, 1000).mapToObj(Row1::new).collect(Collectors.toList());
    private final List<Row1> tenThousandRows = IntStream.range(0, 10000).mapToObj(Row1::new).collect(Collectors.toList());

    @Benchmark
    public void measureConsumeBatchIterator(Blackhole blackhole) throws Exception {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL);
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeCloseAssertingIterator(Blackhole blackhole) throws Exception {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL);
        BatchIterator<Row> itCloseAsserting = new CloseAssertingBatchIterator<>(it);
        while (itCloseAsserting.moveNext()) {
            blackhole.consume(itCloseAsserting.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeSkippingBatchIterator(Blackhole blackhole) throws Exception {
        BatchIterator<Row> it = new InMemoryBatchIterator<>(rows, SENTINEL);
        BatchIterator<Row> skippingIt = new SkippingBatchIterator<>(it, 100);
        while (skippingIt.moveNext()) {
            blackhole.consume(skippingIt.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeNestedLoopJoin(Blackhole blackhole) throws Exception {
        BatchIterator<Row> crossJoin = NestedLoopBatchIterator.crossJoin(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL),
            new CombinedRow(1, 1)
        );
        while (crossJoin.moveNext()) {
            blackhole.consume(crossJoin.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureConsumeNestedLoopLeftJoin(Blackhole blackhole) throws Exception {
        BatchIterator<Row> leftJoin = NestedLoopBatchIterator.leftJoin(
            InMemoryBatchIterator.of(oneThousandRows, SENTINEL),
            InMemoryBatchIterator.of(tenThousandRows, SENTINEL),
            new CombinedRow(1, 1),
            row -> Objects.equals(row.get(0), row.get(1))
        );
        while (leftJoin.moveNext()) {
            blackhole.consume(leftJoin.currentElement().get(0));
        }
        leftJoin.moveToStart();
    }

    @Benchmark
    public void measureSkipLimitSkipLimit(Blackhole blackhole) throws Exception {
        int firstOffset = 1000;
        int secondOffset = 500;
        int firstLimit = 2000;
        int secondLimit = 250;
        BatchIterator<Row> it = LimitingBatchIterator.newInstance(
            new SkippingBatchIterator<>(
                LimitingBatchIterator.newInstance(
                    new SkippingBatchIterator<>(new InMemoryBatchIterator<>(rows, SENTINEL), firstOffset),
                    firstLimit
                ),
                secondOffset
            ),
            secondLimit
        );
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }

    @Benchmark
    public void measureSkipLimit(Blackhole blackhole) throws Exception {
        BatchIterator<Row> it = LimitingBatchIterator.newInstance(
            new SkippingBatchIterator<>(new InMemoryBatchIterator<>(rows, SENTINEL), 1000 + 500),
            250
        );
        while (it.moveNext()) {
            blackhole.consume(it.currentElement().get(0));
        }
    }
}
