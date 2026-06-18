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

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRewriteCast
{
    private final TestableRewriteCast rewriteCast = new TestableRewriteCast();

    private static JdbcTypeHandle newHandle(int jdbcType, String jdbcTypeName)
    {
        return new JdbcTypeHandle(
                jdbcType,
                Optional.ofNullable(jdbcTypeName),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testToJdbcTypeHandleMapsBigintType()
    {
        JdbcTypeHandle source = newHandle(Types.BIGINT, "BIGINT");
        Optional<JdbcTypeHandle> result = rewriteCast.toJdbcTypeHandlePublic(source, BigintType.BIGINT);

        assertThat(result).isPresent();
        JdbcTypeHandle handle = result.get();
        assertThat(handle.jdbcType()).isEqualTo(Types.BIGINT);
        assertThat(handle.jdbcTypeName()).isPresent();
        assertThat(handle.jdbcTypeName().get()).isEqualTo(BigintType.BIGINT.getBaseName());
    }

    @Test
    public void testToJdbcTypeHandleMapsCharTypeWithLength()
    {
        int length = 10;
        JdbcTypeHandle source = newHandle(Types.CHAR, "CHAR");
        CharType charType = CharType.createCharType(length);

        Optional<JdbcTypeHandle> result = rewriteCast.toJdbcTypeHandlePublic(source, charType);

        assertThat(result).isPresent();
        JdbcTypeHandle handle = result.get();
        assertThat(handle.jdbcType()).isEqualTo(Types.CHAR);
        assertThat(handle.columnSize()).isPresent();
        assertThat(handle.columnSize().get()).isEqualTo(length);
    }

    @Test
    public void testToJdbcTypeHandleMapsBoundedVarcharType()
    {
        int bound = 50;
        JdbcTypeHandle source = newHandle(Types.VARCHAR, "VARCHAR");
        VarcharType varcharType = VarcharType.createVarcharType(bound);

        Optional<JdbcTypeHandle> result = rewriteCast.toJdbcTypeHandlePublic(source, varcharType);

        assertThat(result).isPresent();
        JdbcTypeHandle handle = result.get();
        assertThat(handle.jdbcType()).isEqualTo(Types.VARCHAR);
        assertThat(handle.columnSize()).isPresent();
        assertThat(handle.columnSize().get()).isEqualTo(bound);
    }

    @Test
    public void testToJdbcTypeHandleReturnsEmptyForUnboundedVarcharType()
    {
        JdbcTypeHandle source = newHandle(Types.VARCHAR, "VARCHAR");
        VarcharType unbounded = VarcharType.VARCHAR; // unbounded

        Optional<JdbcTypeHandle> result = rewriteCast.toJdbcTypeHandlePublic(source, unbounded);

        // Teradata connector does not support unbounded varchar cast pushdown
        assertThat(result).isEmpty();
    }

    @Test
    public void testToJdbcTypeHandleReturnsEmptyForUnsupportedTarget()
    {
        JdbcTypeHandle source = newHandle(Types.VARCHAR, "VARCHAR");

        Optional<JdbcTypeHandle> result = rewriteCast.toJdbcTypeHandlePublic(source, BooleanType.BOOLEAN);

        assertThat(result).isEmpty();
    }

    @Nested
    public class TestableRewriteCast
            extends RewriteCast
    {
        public TestableRewriteCast()
        {
            super((_, _) -> "");
        }

        public Optional<JdbcTypeHandle> toJdbcTypeHandlePublic(JdbcTypeHandle sourceType, Type targetType)
        {
            return super.toJdbcTypeHandle(sourceType, targetType);
        }
    }
}
