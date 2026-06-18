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
package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.sql.JDBCType;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class RewriteSubstringFunction
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(new FunctionName("substring")))
            .with(type().matching(type -> type instanceof VarcharType))
            .with(argumentCount().matching(count -> count >= 2 && count <= 3))
            .with(argument(0).matching(expression().capturedAs(VALUE).with(type().matching(type -> type instanceof VarcharType))));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Call call = (Call) projectionExpression;
        ConnectorExpression valueExpression = captures.get(VALUE);

        // Get JDBC type handle for the value expression
        JdbcTypeHandle typeHandle = getTypeHandle(valueExpression, context);
        if (typeHandle == null) {
            return Optional.empty();
        }

        // Only rewrite for plain VARCHAR JDBC type named "varchar"
        if (JDBCType.valueOf(typeHandle.jdbcType()) != JDBCType.VARCHAR ||
                !typeHandle.jdbcTypeName().map(name -> name.equalsIgnoreCase("varchar")).orElse(false)) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> value = context.rewriteExpression(valueExpression);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        String expression;
        List<QueryParameter> parameters;
        if (call.getArguments().size() == 2) {
            // Two argument SUBSTRING(value, start)
            Optional<ParameterizedExpression> start = context.rewriteExpression(call.getArguments().get(1));
            if (start.isEmpty()) {
                return Optional.empty();
            }
            expression = format("SUBSTRING(%s FROM %s)", value.get().expression(), start.get().expression());
            parameters = combineParameters(value.get(), start.get());
        }
        else if (call.getArguments().size() == 3) {
            // Three argument SUBSTRING(value, start, length)
            Optional<ParameterizedExpression> start = context.rewriteExpression(call.getArguments().get(1));
            Optional<ParameterizedExpression> length = context.rewriteExpression(call.getArguments().get(2));
            if (start.isEmpty() || length.isEmpty()) {
                return Optional.empty();
            }
            expression = format(
                    "SUBSTRING(%s FROM %s FOR %s)",
                    value.get().expression(),
                    start.get().expression(),
                    length.get().expression());
            parameters = combineParameters(value.get(), start.get(), length.get());
        }
        else {
            return Optional.empty();
        }

        return Optional.of(new JdbcExpression(expression, ImmutableList.copyOf(parameters), typeHandle));
    }

    private JdbcTypeHandle getTypeHandle(ConnectorExpression expression, RewriteContext<ParameterizedExpression> context)
    {
        if (expression instanceof Variable variable) {
            return ((JdbcColumnHandle) context.getAssignment(variable.getName())).getJdbcTypeHandle();
        }
        // For non-variable expressions, we might need to derive the type handle differently
        // This is a simplified approach - you might need more sophisticated type handling
        return new JdbcTypeHandle(JDBCType.VARCHAR.getVendorTypeNumber(), Optional.of("varchar"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private List<QueryParameter> combineParameters(ParameterizedExpression... expressions)
    {
        return Stream.of(expressions)
                .flatMap(expr -> expr.parameters().stream())
                .collect(toList());
    }
}
