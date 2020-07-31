package io.micronaut.discovery.consul.config;

import edu.umd.cs.findbugs.annotations.Nullable;
import io.micronaut.context.annotation.BootstrapContextCompatible;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.*;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.discovery.consul.ConsulConfiguration;
import io.micronaut.discovery.consul.client.v1.ConsulClient;
import io.micronaut.discovery.consul.client.v1.KeyValue;
import io.micronaut.discovery.consul.condition.RequiresConsul;
import io.micronaut.discovery.event.ServiceReadyEvent;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.context.scope.refresh.RefreshEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.Async;
import io.reactivex.*;
import io.reactivex.functions.BooleanSupplier;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.security.Key;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Singleton
@RequiresConsul
@Requires(beans = ConsulClient.class)
@Requires(property = ConsulConfiguration.ConsulWatchesConfiguration.ENABLED, value = StringUtils.TRUE, defaultValue = StringUtils.FALSE)
@BootstrapContextCompatible
public class ConsulConfigurationAutoRefresh {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulConfigurationAutoRefresh.class);

    private final ConsulClient consulClient;
    private final ConsulConfiguration consulConfiguration;
    private final ConsulConfigurationAdapter configurationAdapter;
    private final Map<String, PropertySourceLoader> loaderByFormatMap = new ConcurrentHashMap<>();

    private Environment environment;
    private ExecutorService executionService;
    private final ApplicationEventPublisher eventPublisher;

    private Map<String, Long> watchedKeyprefixes = new ConcurrentHashMap<>();
    private Map<String, Long> keyIndexes = new ConcurrentHashMap<>();

    private String wait = "5m";
    private AtomicBoolean shoulStopOnShutdown = new AtomicBoolean(false);

    /**
     * @param consulClient
     * @param consulConfiguration
     * @param configurationAdapter
     * @param environment
     * @param executorService
     * @param eventPublisher
     */
    // TODO: too much beans in constructor
    public ConsulConfigurationAutoRefresh(ConsulClient consulClient, ConsulConfiguration consulConfiguration, ConsulConfigurationAdapter configurationAdapter, Environment environment, @Named(TaskExecutors.IO) ExecutorService executorService, ApplicationEventPublisher eventPublisher) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing Consul configuration auto refresh");
        }

        this.consulClient = consulClient;
        this.consulConfiguration = consulConfiguration;
        this.configurationAdapter = configurationAdapter;
        this.environment = environment;
        this.executionService = executorService;
        this.eventPublisher = eventPublisher;
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

    @EventListener
    public void onShutdownEvent(ShutdownEvent event) {
        // basically this is graceful shutdown of flowables
        shoulStopOnShutdown.set(true);
    }

    private Flowable<List<KeyValue>> createKeyprefixWatcher(String keyprefix, String dc, String wait) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating watcher for keyprefix " + keyprefix + " with wait interval " + wait);
        }

        final AtomicBoolean shouldFinishOnIsolatedReason = new AtomicBoolean(false);

        /**
         * Create key prefix isolated condition when to stop repeating flowable that fetches for
         * prefixed keyvalues.
         */
        // TODO: what are other events that may cause stop repeating this: change of consul.client.watches.enabled: true -> false
        BooleanSupplier shouldStopRepating = new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() throws Exception {
                // stop on service shutdown or on specific error condidtion configured within the flowable
                return shouldFinishOnIsolatedReason.get() || shoulStopOnShutdown.get();
            }
        };


        Function<Throwable, Publisher<? extends List<KeyValue>>> errorHandler = throwable -> {
            if (throwable instanceof HttpClientResponseException) {
                HttpClientResponseException httpClientResponseException = (HttpClientResponseException) throwable;
                if (httpClientResponseException.getStatus() == HttpStatus.NOT_FOUND) {
                    // in case configuration is not found then we should stop watching it
                    // TODO: should we? previously it was evidently there so this can be an user error
                    // TODO: what are responses that may cause we want to stop ?
                    shouldFinishOnIsolatedReason.set(true);
                }
            }
            if (LOG.isWarnEnabled()) {
                LOG.warn("S......");
            }
            return Flowable.just(new ArrayList<>());
        };


        return Flowable.create((FlowableOnSubscribe<List<KeyValue>>) emitter -> {
            Long index = watchedKeyprefixes.get(keyprefix);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Watch for changes under keyprefix " + keyprefix + " from index " + index);
            }

            List<KeyValue> keyValues = Flowable.fromPublisher(
                    consulClient.readValues(keyprefix, dc, null, null, wait, index))
                    .doOnError(
                            throwable -> LOG.error("Error while watching for prefix " + keyprefix + " changes", throwable)
                    )
                    .subscribeOn(Schedulers.from(executionService))
                    .onErrorReturnItem(new ArrayList<>())
                    .blockingFirst(new ArrayList<>());

            if (!CollectionUtils.isEmpty(keyValues)) {
                Long nextIndex = resolveModifiedKeyIndex(keyValues);
                if (nextIndex > index) {
                    watchedKeyprefixes.put(keyprefix, nextIndex);
                    emitter.onNext(keyValues);
                } else if (nextIndex < index) {
                    //
                    // * Reset the index if goes backwards.
                    // * See https://www.consul.io/api-docs/features/blocking#implementation-details
                    //
                    watchedKeyprefixes.put(keyprefix, 0L);
                }
            }
            // always finish and let repeatUntil to it's job
            emitter.onComplete();

        }, BackpressureStrategy.BUFFER)
                .repeatUntil(shouldStopRepating)
                .onErrorResumeNext(errorHandler);
    }


    /**
     * TODO: Didn't found yet how to receive HTTP headers from Publisher yet, so for POC this way

     */
    public static Long resolveModifiedKeyIndex(List<KeyValue> keyValues) {
        if (keyValues != null && !keyValues.isEmpty()) {
            Optional<KeyValue> max = keyValues.stream().max(Comparator.comparing(KeyValue::getModifyIndex));
            Long respIndex = max.get().getModifyIndex();
            if (respIndex > 0) {
                return respIndex;
            }
        }
        return 0L;
    }

    @EventListener
    // TODO: or StartupEvent ?
    public void onServiceReadyEvent(ServiceReadyEvent event) {
        ConsulConfiguration.ConsulConfigDiscoveryConfiguration configDiscoveryConfiguration = consulConfiguration.getConfiguration();
        String dc = configDiscoveryConfiguration.getDatacenter().orElse(null);
        Scheduler scheduler = Schedulers.from(executionService);

        /*
         * resolve watched keyprefixes with respective keys and their indexes for further use
         * TODO: what is the drawback of autorefresh, can common or app prefix change?
         *  are there any other prefixes? What about observing whole /config
         */
        String applicationSpecificPrefix = ConsulConfigurationUtil.getApplicationSpecificConfigPrefix(consulConfiguration);
        watchedKeyprefixes.put(applicationSpecificPrefix, 0L);

        String commonConfigPrefix = ConsulConfigurationUtil.getCommonConfigPrefix(consulConfiguration);
        watchedKeyprefixes.put(commonConfigPrefix, 0L);

        for (String keyprefix : watchedKeyprefixes.keySet()) {
            List<KeyValue> keyValues = Flowable.fromPublisher(consulClient.readValues(keyprefix, null, null, null, null, null))
                    .blockingFirst();

            // initialise index for keyprefix
            Long maxIndex = resolveModifiedKeyIndex(keyValues);
            watchedKeyprefixes.put(keyprefix, maxIndex);

            // initialise indexes for specific keys in keyprefix
            keyValues
                    .stream()
                    .map(keyValue -> keyIndexes.put(keyValue.getKey(), keyValue.getModifyIndex()));
        }

        /*
         * generate keyprefix watchers that should run idenfinitely
         * and process their results
         * TODO: what when someone registers new watcher after startup?
         */
        List<Flowable<List<KeyValue>>> keyprefixWatchers = new ArrayList<>();
        for (String prefix : watchedKeyprefixes.keySet()) {
            keyprefixWatchers.add(createKeyprefixWatcher(prefix, dc, wait)
                    .subscribeOn(scheduler));
        }

        // Merge keyprefix watchers and subscribe to them by processing results
        Flowable.merge(keyprefixWatchers)
                .subscribeOn(scheduler)
                .subscribe(this::processResult);
    }


    /**
     * TODO: what about keys removed properties?
     * TODO: Option 1] Brute force - always refresh completely prefix tree
     * TODO: Option 2] Smart way - check what keys is missing from propertyIndexes
     *
     * @param keyvalues
     */
    private void processResult(List<KeyValue> keyvalues) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received updated configuration");
        }

        boolean shouldRefresh = false;
        for (KeyValue keyValue : keyvalues) {
            Long existingKey = keyIndexes.getOrDefault(keyValue.getKey(), 0L);  // ) means it's new property

            // process new and changed keys
            if (existingKey == 0L || existingKey < keyValue.getModifyIndex()) {
                shouldRefresh = true;

                List<PropertySource> propertySources = configurationAdapter.keyvalueToPropertySources(keyValue);
                for (PropertySource propertySource : propertySources) {
                    if (environment.containsProperty(propertySource.getName())) {
                        environment.removePropertySource(propertySource);
                    }
                    environment.addPropertySource(propertySource);
                }
            }
        }

        if (shouldRefresh) {
            Map<String, Object> changes = environment.refreshAndDiff();
            if (!changes.isEmpty()) {
                eventPublisher.publishEvent(new RefreshEvent(changes));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Changed properties: " + String.join(",", changes.keySet()));
            }
        }
    }
}
