package io.micronaut.discovery.consul.config;

import io.micronaut.context.env.Environment;
import io.micronaut.discovery.config.ConfigDiscoveryConfiguration;
import io.micronaut.discovery.consul.ConsulConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConsulConfigurationUtil {

    public static final String getApplicationSpecificConfigPrefix(ConsulConfiguration consulConfiguration){
        Optional<String> serviceId = consulConfiguration.getServiceId();
        ConsulConfiguration.ConsulConfigDiscoveryConfiguration configDiscoveryConfiguration = consulConfiguration.getConfiguration();
        String path = configDiscoveryConfiguration.getPath().orElse(ConfigDiscoveryConfiguration.DEFAULT_PATH);
        if (!path.endsWith("/")) {
            path += "/";
        }

        final boolean hasApplicationSpecificConfig = serviceId.isPresent();
        return hasApplicationSpecificConfig ? path + serviceId.get() : null;
    }

    public static final String getCommonConfigPrefix(ConsulConfiguration consulConfiguration){
        ConsulConfiguration.ConsulConfigDiscoveryConfiguration configDiscoveryConfiguration = consulConfiguration.getConfiguration();
        String path = configDiscoveryConfiguration.getPath().orElse(ConfigDiscoveryConfiguration.DEFAULT_PATH);
        if (!path.endsWith("/")) {
            path += "/";
        }
        return path + Environment.DEFAULT_NAME;
    }
}
