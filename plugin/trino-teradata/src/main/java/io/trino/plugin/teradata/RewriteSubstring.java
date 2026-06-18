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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class RewriteSubstring
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<ConnectorExpression> START = newCapture();

    public static final FunctionName SUBSTRING_FUNCTION_NAME = new FunctionName("substring");

    @Override
    public Pattern<Call> getPattern()
    {
        return call()
                .with(functionName().equalTo(SUBSTRING_FUNCTION_NAME))
                .with(argumentCount().matching(count -> count == 2 || count == 3))
                .with(argument(0).matching(expression().capturedAs(VALUE)))
                .with(argument(1).matching(expression().capturedAs(START)));
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Optional<ParameterizedExpression> value = context.defaultRewrite(captures.get(VALUE));
        Optional<ParameterizedExpression> start = context.defaultRewrite(captures.get(START));

        if (value.isEmpty() || start.isEmpty()) {
            return Optional.empty();
        }

        if (call.getArguments().size() == 3) {
            Optional<ParameterizedExpression> length = context.defaultRewrite(call.getArguments().get(2));
            if (length.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ParameterizedExpression(
                    format("SUBSTRING(%s FROM %s FOR %s)",
                            value.get().expression(),
                            start.get().expression(),
                            length.get().expression()),
                    combineParameters(value.get(), start.get(), length.get())));
        }
        else {
            return Optional.of(new ParameterizedExpression(
                    format("SUBSTRING(%s FROM %s)",
                            value.get().expression(),
                            start.get().expression()),
                    combineParameters(value.get(), start.get())));
        }
    }

    private List<QueryParameter> combineParameters(ParameterizedExpression... expressions)
    {
        return Stream.of(expressions)
                .flatMap(expr -> expr.parameters().stream())
                .collect(toList());
    }
}
