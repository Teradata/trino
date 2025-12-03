package io.trino.plugin.teradata.integration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestTeradataClassLoading
{
    @Test
    void testPluginClassLoadable() throws Exception
    {
        Class<?> pluginClass = Class.forName("io.trino.plugin.teradata.TeradataPlugin");
        Object pluginInstance = pluginClass.getDeclaredConstructor().newInstance();
        assertThat(pluginInstance).isNotNull();
    }
}
