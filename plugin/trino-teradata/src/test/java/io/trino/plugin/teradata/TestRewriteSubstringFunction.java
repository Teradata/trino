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

import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRewriteSubstringFunction
{
    private final RewriteSubstringFunction rewriteSubstringFunction = new RewriteSubstringFunction();

    @Test
    public void testPatternMatchesSubstringFunctionWithTwoArguments()
    {
        Variable value = new Variable("test_column", VARCHAR);
        Constant start = new Constant(1L, IntegerType.INTEGER);

        Call substringCall = new Call(
                VARCHAR,
                new FunctionName("substring"),
                List.of(value, start));
        boolean matches = rewriteSubstringFunction.getPattern().match(substringCall).findFirst().isPresent();
        assertThat(matches).isTrue();
    }

    @Test
    public void testPatternMatchesSubstringFunctionWithThreeArguments()
    {
        Variable value = new Variable("test_column", VARCHAR);
        Constant start = new Constant(1L, IntegerType.INTEGER);
        Constant length = new Constant(5L, IntegerType.INTEGER);

        Call substringCall = new Call(
                VARCHAR,
                new FunctionName("substring"),
                List.of(value, start, length));
        boolean matches = rewriteSubstringFunction.getPattern().match(substringCall).findFirst().isPresent();
        assertThat(matches).isTrue();
    }

    @Test
    public void testPatternDoesNotMatchNonVarcharFirstArgument()
    {
        Variable value = new Variable("test_column", IntegerType.INTEGER);
        Constant start = new Constant(1L, IntegerType.INTEGER);

        Call substringCall = new Call(
                VARCHAR,
                new FunctionName("substring"),
                List.of(value, start));
        boolean matches = rewriteSubstringFunction.getPattern().match(substringCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }

    @Test
    public void testPatternDoesNotMatchOtherFunctions()
    {
        Variable value = new Variable("test_column", VARCHAR);
        Constant start = new Constant(1L, IntegerType.INTEGER);

        Call lowerCall = new Call(
                VARCHAR,
                new FunctionName("lower"),
                List.of(value, start));
        boolean matches = rewriteSubstringFunction.getPattern().match(lowerCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }

    @Test
    public void testPatternDoesNotMatchWithOneArgument()
    {
        Variable value = new Variable("test_column", VARCHAR);

        Call substringCall = new Call(
                VARCHAR,
                new FunctionName("substring"),
                List.of(value));
        boolean matches = rewriteSubstringFunction.getPattern().match(substringCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }

    @Test
    public void testPatternDoesNotMatchWithFourArguments()
    {
        Variable value = new Variable("test_column", VARCHAR);
        Constant start = new Constant(1L, IntegerType.INTEGER);
        Constant length = new Constant(5L, IntegerType.INTEGER);
        Constant extra = new Constant(10L, IntegerType.INTEGER);

        Call substringCall = new Call(
                VARCHAR,
                new FunctionName("substring"),
                List.of(value, start, length, extra));
        boolean matches = rewriteSubstringFunction.getPattern().match(substringCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }

    @Test
    public void testPatternDoesNotMatchEmptyArguments()
    {
        Call substringCall = new Call(
                VARCHAR,
                new FunctionName("substring"),
                List.of());
        boolean matches = rewriteSubstringFunction.getPattern().match(substringCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }
}
