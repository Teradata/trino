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
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static java.sql.Types.BIGINT;
import static java.sql.Types.CHAR;
import static java.sql.Types.INTEGER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;

public class RewriteCast
        extends AbstractRewriteCast
{
    private static final List<Integer> SUPPORTED_SOURCE_TYPE_FOR_INTEGRAL_CAST = ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT);

    public RewriteCast(BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        super(jdbcTypeProvider);
    }

    private static boolean pushdownSupported(JdbcTypeHandle sourceType, Type targetType)
    {
        return switch (targetType) {
            case SmallintType _, IntegerType _, BigintType _ -> SUPPORTED_SOURCE_TYPE_FOR_INTEGRAL_CAST.contains(sourceType.jdbcType());
            case CharType _ -> supportedSourceTypeToCastToChar(sourceType);
            case VarcharType _ -> supportedSourceTypeToCastToVarchar(sourceType);
            default -> false;
        };
    }

    private static boolean supportedSourceTypeToCastToChar(JdbcTypeHandle sourceType)
    {
        return switch (sourceType.jdbcType()) {
            case Types.CHAR,
                 Types.VARCHAR -> true;
            default -> false;
        };
    }

    private static boolean supportedSourceTypeToCastToVarchar(JdbcTypeHandle sourceType)
    {
        return sourceType.jdbcType() == VARCHAR;
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(JdbcTypeHandle sourceType, Type targetType)
    {
        if (!pushdownSupported(sourceType, targetType)) {
            return Optional.empty();
        }
        return switch (targetType) {
            case SmallintType smallintType -> Optional.of(new JdbcTypeHandle(SMALLINT, Optional.of(smallintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case IntegerType integerType -> Optional.of(new JdbcTypeHandle(INTEGER, Optional.of(integerType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case BigintType bigintType -> Optional.of(new JdbcTypeHandle(BIGINT, Optional.of(bigintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case CharType charType -> Optional.of(new JdbcTypeHandle(CHAR, Optional.of(charType.getBaseName()), Optional.of(charType.getLength()), Optional.empty(), Optional.empty(), Optional.empty()));
            case VarcharType varcharType -> {
                if (!varcharType.isUnbounded()) {
                    yield Optional.of(new JdbcTypeHandle(VARCHAR, Optional.of(varcharType.getBaseName()), varcharType.getLength(), Optional.empty(), Optional.empty(), Optional.empty()));
                }
                else {
                    // Teradata does not support unbounded varchar, so not pushing down the cast
                    yield Optional.empty();
                }
            }
            default -> Optional.empty();
        };
    }
}
