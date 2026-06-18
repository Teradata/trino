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
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRewriteUpper
{
    private final RewriteUpper rewriteUpper = new RewriteUpper();

    @Test
    public void testPatternMatchesUpperFunction()
    {
        Variable variable = new Variable("test_column", VarcharType.VARCHAR);
        Call upperCall = new Call(
                VarcharType.VARCHAR,
                new FunctionName("upper"),
                List.of(variable));
        boolean matches = rewriteUpper.getPattern().match(upperCall).findFirst().isPresent();
        assertThat(matches).isTrue();
    }

    @Test
    public void testPatternDoesNotMatchOtherFunctions()
    {
        Variable variable = new Variable("test_column", VarcharType.VARCHAR);
        Call lowerCall = new Call(
                VarcharType.VARCHAR,
                new FunctionName("lower"),
                List.of(variable));
        boolean matches = rewriteUpper.getPattern().match(lowerCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }

    @Test
    public void testPatternDoesNotMatchWrongArgumentCount()
    {
        Variable variable1 = new Variable("col1", VarcharType.VARCHAR);
        Variable variable2 = new Variable("col2", VarcharType.VARCHAR);
        Call upperCall = new Call(
                VarcharType.VARCHAR,
                new FunctionName("upper"),
                List.of(variable1, variable2));
        boolean matches = rewriteUpper.getPattern().match(upperCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }

    @Test
    public void testPatternDoesNotMatchEmptyArguments()
    {
        Call upperCall = new Call(
                VarcharType.VARCHAR,
                new FunctionName("upper"),
                List.of());
        boolean matches = rewriteUpper.getPattern().match(upperCall).findFirst().isPresent();
        assertThat(matches).isFalse();
    }
}
