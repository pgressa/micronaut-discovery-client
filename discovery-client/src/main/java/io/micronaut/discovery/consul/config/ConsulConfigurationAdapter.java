package io.micronaut.discovery.consul.config;

/**
 * Adapter class that transforms Consul value types into property sources.
 */

import io.micronaut.context.annotation.BootstrapContextCompatible;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.*;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.discovery.client.ClientUtil;
import io.micronaut.discovery.config.ConfigDiscoveryConfiguration;
import io.micronaut.discovery.config.ConfigurationClient;
import io.micronaut.discovery.consul.ConsulConfiguration;
import io.micronaut.discovery.consul.client.v1.ConsulClient;
import io.micronaut.discovery.consul.client.v1.KeyValue;
import io.micronaut.discovery.consul.condition.RequiresConsul;
import io.micronaut.jackson.env.JsonPropertySourceLoader;

import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
@Singleton
@RequiresConsul
@Requires(property = ConfigurationClient.ENABLED, value = StringUtils.TRUE, defaultValue = StringUtils.FALSE)
@BootstrapContextCompatible
public class ConsulConfigurationAdapter {

    private final ConsulConfiguration consulConfiguration;
    private final Environment environment;
    private final Map<String, PropertySourceLoader> loaderByFormatMap = new ConcurrentHashMap<>();

    //TODO: Is injection environment in constructor OK or do we need to get inject it dynamically for every processing?
    public ConsulConfigurationAdapter(ConsulConfiguration consulConfiguration, Environment environment) {
        this.environment = environment;
        this.consulConfiguration = consulConfiguration;

        if (environment != null) {
            Collection<PropertySourceLoader> loaders = environment.getPropertySourceLoaders();
            for (PropertySourceLoader loader : loaders) {
                Set<String> extensions = loader.getExtensions();
                for (String extension : extensions) {
                    loaderByFormatMap.put(extension, loader);
                }
            }
        }
    }

    public List<PropertySource> keyvalueToPropertySources(KeyValue keyValue) {
        Base64.Decoder base64Decoder = Base64.getDecoder();

        Map<String, ConsulConfigurationAdapter.LocalSource> propertySources = new HashMap<>();

        List<String> activeNames = new ArrayList<>(environment.getActiveNames());
        Optional<String> serviceId = consulConfiguration.getServiceId();
        ConsulConfiguration.ConsulConfigDiscoveryConfiguration configDiscoveryConfiguration = consulConfiguration.getConfiguration();

        ConfigDiscoveryConfiguration.Format format = configDiscoveryConfiguration.getFormat();
        String path = configDiscoveryConfiguration.getPath().orElse(ConfigDiscoveryConfiguration.DEFAULT_PATH);
        if (!path.endsWith("/")) {
            path += "/";
        }

        // resolve common configuration path
        String pathPrefix = path;
        String commonConfigPath = path + Environment.DEFAULT_NAME;

        // resolve application specific path
        final boolean hasApplicationSpecificConfig = serviceId.isPresent();
        String applicationSpecificPath = hasApplicationSpecificConfig ? path + serviceId.get() : null;

        // resolve the key
        String key = keyValue.getKey();
        String value = keyValue.getValue();
        boolean isFolder = key.endsWith("/") && value == null;
        boolean isCommonConfigKey = key.startsWith(commonConfigPath);
        boolean isApplicationSpecificConfigKey = hasApplicationSpecificConfig && key.startsWith(applicationSpecificPath);
        boolean validKey = isCommonConfigKey || isApplicationSpecificConfigKey;
        if (!isFolder && validKey) {
            switch (format) {
                case FILE:
                    String fileName = key.substring(pathPrefix.length());
                    int i = fileName.lastIndexOf('.');
                    if (i > -1) {
                        String ext = fileName.substring(i + 1);
                        fileName = fileName.substring(0, i);
                        PropertySourceLoader propertySourceLoader = resolveLoader(ext);
                        if (propertySourceLoader != null) {
                            String propertySourceName = resolvePropertySourceName(Environment.DEFAULT_NAME, fileName, activeNames);
                            if (hasApplicationSpecificConfig && propertySourceName == null) {
                                propertySourceName = resolvePropertySourceName(serviceId.get(), fileName, activeNames);
                            }
                            if (propertySourceName != null) {
                                String finalName = propertySourceName;
                                byte[] decoded = base64Decoder.decode(value);
                                Map<String, Object> properties = propertySourceLoader.read(propertySourceName, decoded);
                                String envName = ClientUtil.resolveEnvironment(fileName, activeNames);
                                ConsulConfigurationAdapter.LocalSource localSource = propertySources.computeIfAbsent(propertySourceName, s -> new ConsulConfigurationAdapter.LocalSource(isApplicationSpecificConfigKey, envName, finalName));
                                localSource.putAll(properties);
                            }
                        }
                    }
                    break;

                case NATIVE:
                    String property = null;
                    Set<String> propertySourceNames = null;
                    if (isCommonConfigKey) {
                        property = resolvePropertyName(commonConfigPath, key);
                        propertySourceNames = resolvePropertySourceNames(pathPrefix, key, activeNames);

                    } else if (isApplicationSpecificConfigKey) {
                        property = resolvePropertyName(applicationSpecificPath, key);
                        propertySourceNames = resolvePropertySourceNames(pathPrefix, key, activeNames);
                    }
                    if (property != null && propertySourceNames != null) {
                        for (String propertySourceName : propertySourceNames) {
                            String envName = ClientUtil.resolveEnvironment(propertySourceName, activeNames);
                            ConsulConfigurationAdapter.LocalSource localSource = propertySources.computeIfAbsent(propertySourceName, s -> new ConsulConfigurationAdapter.LocalSource(isApplicationSpecificConfigKey, envName, propertySourceName));
                            byte[] decoded = base64Decoder.decode(value);
                            localSource.put(property, new String(decoded));
                        }
                    }
                    break;

                case JSON:
                case YAML:
                case PROPERTIES:
                    String fullName = key.substring(pathPrefix.length());
                    if (!fullName.contains("/")) {
                        propertySourceNames = ClientUtil.calcPropertySourceNames(fullName, activeNames, ",");
                        String formatName = format.name().toLowerCase(Locale.ENGLISH);
                        PropertySourceLoader propertySourceLoader = resolveLoader(formatName);

                        if (propertySourceLoader == null) {
                            throw new ConfigurationException("No PropertySourceLoader found for format [" + format + "]. Ensure ConfigurationClient is running within Micronaut container.");
                        } else {
                            if (propertySourceLoader.isEnabled()) {
                                byte[] decoded = base64Decoder.decode(value);
                                Map<String, Object> properties = propertySourceLoader.read(fullName, decoded);
                                for (String propertySourceName : propertySourceNames) {
                                    String envName = ClientUtil.resolveEnvironment(propertySourceName, activeNames);
                                    ConsulConfigurationAdapter.LocalSource localSource = propertySources.computeIfAbsent(propertySourceName, s -> new ConsulConfigurationAdapter.LocalSource(isApplicationSpecificConfigKey, envName, propertySourceName));
                                    localSource.putAll(properties);
                                }
                            }
                        }
                    }
                    break;
                default:
                    // no-op
            }

            int basePriority = EnvironmentPropertySource.POSITION + 100;
            int envBasePriority = basePriority + 50;

            List<PropertySource> propertySourceList = new ArrayList<>(propertySources.size());

            for (ConsulConfigurationAdapter.LocalSource localSource: propertySources.values()) {
                int priority;
                if (localSource.environment != null) {
                    priority = envBasePriority + (activeNames.indexOf(localSource.environment) * 2);
                } else {
                    priority = basePriority + 1;
                }
                if (localSource.appSpecific) {
                    priority++;
                }
                propertySourceList.add(PropertySource.of(ConsulClient.SERVICE_ID + '-' + localSource.name, localSource.values, priority));
            }

            return propertySourceList;
        }else{
            return new ArrayList<>(0);
        }
    }

    private String resolvePropertySourceName(String rootName, String fileName, List<String> activeNames) {
        String propertySourceName = null;
        if (fileName.startsWith(rootName)) {
            String envString = fileName.substring(rootName.length());
            if (StringUtils.isEmpty(envString)) {
                propertySourceName = rootName;
            } else if (envString.startsWith("-")) {
                String env = envString.substring(1);
                if (activeNames.contains(env)) {
                    propertySourceName = rootName + '[' + env + ']';
                }
            }
        }
        return propertySourceName;
    }

    private PropertySourceLoader resolveLoader(String formatName) {
        return loaderByFormatMap.computeIfAbsent(formatName, f -> defaultLoader(formatName));
    }

    private PropertySourceLoader defaultLoader(String format) {
        try {
            switch (format) {
                case "json":
                    return new JsonPropertySourceLoader();
                case "properties":
                    return new PropertiesPropertySourceLoader();
                case "yml":
                case "yaml":
                    return new YamlPropertySourceLoader();
                default:
                    // no-op
            }
        } catch (Exception e) {
            // ignore, fallback to exception
        }
        throw new ConfigurationException("Unsupported properties file format: " + format);
    }

    private Set<String> resolvePropertySourceNames(String finalPath, String key, List<String> activeNames) {
        Set<String> propertySourceNames = null;
        String prefix = key.substring(finalPath.length());
        int i = prefix.indexOf('/');
        if (i > -1) {
            prefix = prefix.substring(0, i);
            propertySourceNames = ClientUtil.calcPropertySourceNames(prefix, activeNames, ",");
            if (propertySourceNames == null) {
                return null;
            }
        }
        return propertySourceNames;
    }

    private String resolvePropertyName(String commonConfigPath, String key) {
        String property = key.substring(commonConfigPath.length());

        if (StringUtils.isNotEmpty(property)) {
            if (property.charAt(0) == '/') {
                property = property.substring(1);
            } else if (property.lastIndexOf('/') > -1) {
                property = property.substring(property.lastIndexOf('/') + 1);
            }
        }
        if (property.indexOf('/') == -1) {
            return property;
        }
        return null;
    }

    /**
     * A local property source.
     */
    private static class LocalSource {

        private final boolean appSpecific;
        private final String environment;
        private final String name;
        private final Map<String, Object> values = new LinkedHashMap<>();

        LocalSource(boolean appSpecific,
                    String environment,
                    String name) {
            this.appSpecific = appSpecific;
            this.environment = environment;
            this.name = name;
        }

        void put(String key, Object value) {
            this.values.put(key, value);
        }

        void putAll(Map<String, Object> values) {
            this.values.putAll(values);
        }

    }


}
