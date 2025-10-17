/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acme.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class Routes extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("timer:foo?period={{timer.period}}&delay={{timer.delay}}")
                .routeId("generate-order")
                .bean(OrderService.class, "generateOrder")
                .to("direct:send-to-broker");

        from("direct:send-to-broker")
                .choice()
                        .when(simple("${body.item} == 'Camel'"))
                                .log("Processing a camel book")
                                .marshal().json() // convert to JSON
                                .to("kafka:camel-book?requestRequiredAcks=all") // send messages to kafka with ack = all
                        .otherwise()
                                .log("Processing a Strimzi book")
                                .marshal().jacksonXml()
                        .to("kafka:strimzi-book?requestRequiredAcks=all"); // send messages to kafka with ack = all

        // kafka consumer from camel-book topic
        from("kafka:camel-book?groupId=camel-book")
                .routeId("kafka-consumer-camel")
                .log("[Camel] Message from Kafka: ${body}");

        // kafka consumer from strimzi-book topic 
        from("kafka:strimzi-book?groupId=strimzi-book")
                .routeId("kafka-consumer-strimzi")
                .log("[Strimzi] Message from Kafka: ${body}");

    }
}
