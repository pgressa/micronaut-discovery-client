/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.discovery.consul.config

import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import io.micronaut.discovery.DiscoveryClient
import io.micronaut.discovery.ServiceInstance
import io.micronaut.discovery.config.ConfigurationClient
import io.micronaut.discovery.consul.MockConsulServer
import io.micronaut.discovery.consul.client.v1.ConsulClient
import io.micronaut.discovery.consul.client.v1.KeyValue
import io.micronaut.discovery.consul.config.ConsulConfigurationAutoRefresh
import io.micronaut.discovery.event.ServiceReadyEvent
import io.micronaut.runtime.server.EmbeddedServer
import io.reactivex.Flowable
import io.reactivex.functions.BooleanSupplier
import org.testcontainers.containers.GenericContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

/**
 */
class ConsulConfigurationAutoRefreshSpec extends Specification {
/*
    @Shared
    @AutoCleanup
    GenericContainer consulContainer =
            new GenericContainer("consul:latest")
                    .withExposedPorts(8500)
*/
    @Shared
    String consulHost
    @Shared
    int consulPort

    def setupSpec() {
        //consulContainer.start()
        //consulHost = consulContainer.containerIpAddress
        //consulPort = consulContainer.getMappedPort(8500)
        consulHost = "localhost"
        consulPort = 8500
    }

    void 'test that auto refresh works'() {
        when: "A new server is bootstrapped"
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer,
                ['micronaut.application.name'     : 'test-auto-reg',
                 'consul.client.host'             : consulHost,
                 'consul.client.port'             : consulPort,
                 'consul.client.enabled'          : true,
                 'consul.client.watcher.enabled'  : true,
                 'consul.client.registration.enabled': false,
                 'micronaut.config-client.enabled': true])

        then: "The bean was created"
        ConsulConfigurationAutoRefresh autoRefresh = embeddedServer.applicationContext.getBean(ConsulConfigurationAutoRefresh)
        autoRefresh != null

        ConsulClient client = embeddedServer.applicationContext.getBean(ConsulClient)
        autoRefresh != null

        when:
        System.out.println("GOING TO CHANGE THE VALUE")
        Flowable.fromPublisher(client.putValue("config/test-auto-reg/foo/bar", "122")).blockingFirst()


        sleep(30 * 1000)
        then: "the key values is registered with Consul"
        Map<String, Object> map = embeddedServer.applicationContext.getEnvironment().getProperties("test-auto-reg")
        for(Map.Entry<String, Object> entry : map.entrySet()){
            System.out.println(entry.key + "  " + entry.value);
        }

        embeddedServer.stop()
    }


    void 'test blocking query'() {
        when: "A new server is bootstrapped"
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer,
                ['micronaut.application.name'     : 'test-auto-reg',
                 'consul.client.host'             : consulHost,
                 'consul.client.port'             : consulPort,
                 'consul.client.enabled'          : true,
                 'consul.client.watcher.enabled'  : false,
                 'consul.client.registration.enabled': false,
                 'micronaut.config-client.enabled': true])

        then: "The bean was created"
        ConsulClient client = embeddedServer.applicationContext.getBean(ConsulClient)

        when:
        System.out.println("GOING TO CHANGE THE VALUE")
        Flowable.fromPublisher(client.putValue("config/test-auto-reg/foo/zoo/bar", "122")).blockingFirst()
        List<KeyValue> valueList = Flowable.fromPublisher(client.readValues("config/test-auto-reg/foo/zoo")).blockingFirst()
        Long index = ConsulConfigurationAutoRefresh.resolveModifiedKeyIndex(valueList)
        List<KeyValue> list = Flowable.fromPublisher(client.readValues("config/test-auto-reg/foo/zoo", null, null, null, null, index)).blockingFirst()

        then:
        System.out.println("LIST Out " + list)
        list.size() == 1

        embeddedServer.stop()
    }




    private void writeValue(String env, String name, String value) {
        Flowable.fromPublisher(client.putValue("/config/$env/$name", value)).blockingFirst()
    }
}
