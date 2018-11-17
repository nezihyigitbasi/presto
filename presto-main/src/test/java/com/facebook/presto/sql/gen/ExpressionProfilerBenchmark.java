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
package com.facebook.presto.sql.gen;

import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProcessorOutput;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(5)
@Warmup(iterations = 10, time = 60, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 60, timeUnit = SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class ExpressionProfilerBenchmark
{
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR);
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final int POSITIONS = 1024;

    private final DriverYieldSignal yieldSignal = new DriverYieldSignal();
    private final Map<Symbol, Type> symbolTypes = new HashMap<>();
    private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

    private PageProcessor pageProcessor;
    private Page inputPage;
    private List<Page> inputPages;
    private List<Type> types;

    int columnCount = 8;

    //    @Param({"varchar", "bigint"})
    String type = "bigint";

    @Param({"false", "true"})
    boolean expressionProfilerEnabled;

    private Optional<Page> next;

    @Setup
    public void setup()
    {
        Type type = TYPE_MAP.get(this.type);

        for (int i = 0; i < columnCount; i++) {
            Symbol symbol = new Symbol(type.getDisplayName().toLowerCase(ENGLISH) + i);
            symbolTypes.put(symbol, type);
            sourceLayout.put(symbol, i);
        }

        List<RowExpression> projections = getProjections(type);
        types = projections.stream().map(RowExpression::getType).collect(toList());

        MetadataManager metadata = createTestMetadataManager();
        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);

        inputPage = createPages(types, POSITIONS, 1).get(0);
        inputPages = createPages(types, POSITIONS, 8 * 1024);
        pageProcessor = new ExpressionCompiler(metadata, pageFunctionCompiler).compilePageProcessor(Optional.of(getFilter(type)), projections, Optional.empty(), expressionProfilerEnabled).get();
    }

    @Benchmark
    public void pageProjection(Blackhole blackhole)
    {
        PageProcessorOutput output = pageProcessor.process(null, yieldSignal, inputPage);
        while (output.hasNext()) {
            blackhole.consume(output.next());
        }
    }

    public void pageProjection()
    {
        for (Page page : inputPages) {
            PageProcessorOutput output = pageProcessor.process(null, yieldSignal, page);
            while (output.hasNext()) {
                next = output.next();
            }
        }
    }

    private RowExpression getFilter(Type type)
    {
        if (type == VARCHAR) {
            return rowExpression("cast(varchar0 as bigint) % 2 = 0", VARCHAR);
        }
        if (type == BIGINT) {
            return rowExpression("bigint0 % 2 = 0", BIGINT);
        }
        throw new IllegalArgumentException("filter not supported for type : " + type);
    }

    private List<RowExpression> getProjections(Type type)
    {
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        if (type == BIGINT) {
            for (int i = 0; i < columnCount; i++) {
                builder.add(rowExpression("bigint" + i + " + 5", type));
            }
        }
        else if (type == VARCHAR) {
            for (int i = 0; i < columnCount; i++) {
                // alternatively use identity expression rowExpression("varchar" + i, type) or
                // rowExpression("substr(varchar" + i + ", 1, 1)", type)
                builder.add(rowExpression("concat(varchar" + i + ", 'foo')", type));
            }
        }
        return builder.build();
    }

    private RowExpression rowExpression(String expression, Type type)
    {
        SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
        Expression inputReferenceExpression = symbolToInputRewriter.rewrite(createExpression(expression, METADATA, TypeProvider.copyOf(symbolTypes)));

        ImmutableMap.Builder<Integer, Type> builder = ImmutableMap.builder();
        for (int i = 0; i < columnCount; i++) {
            builder.put(i, type);
        }
        Map<Integer, Type> types = builder.build();

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(TEST_SESSION, METADATA, SQL_PARSER, types, inputReferenceExpression, emptyList(), WarningCollector.NOOP);
        return SqlToRowExpressionTranslator.translate(inputReferenceExpression, SCALAR, expressionTypes, METADATA.getFunctionRegistry(), METADATA.getTypeManager(), TEST_SESSION, true);
    }

    private static List<Page> createPages(List<? extends Type> types, int positionCount, int pageCount)
    {
        ImmutableList.Builder builder = new ImmutableList.Builder();
        for (int i = 0; i < pageCount; i++) {
            builder.add(SequencePageBuilder.createSequencePage(types, positionCount));
        }
        return builder.build();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + ExpressionProfilerBenchmark.class.getSimpleName() + ".*")
                .jvmArgs("-Xmx8g")
                .build();

//        ExpressionProfilerBenchmark benchmark = new ExpressionProfilerBenchmark();
//        benchmark.setup();
//        benchmark.pageProjection();

        new Runner(options).run();
    }
}