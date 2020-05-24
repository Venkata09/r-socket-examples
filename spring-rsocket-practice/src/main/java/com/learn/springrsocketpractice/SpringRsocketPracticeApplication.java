package com.learn.springrsocketpractice;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SpringBootApplication
public class SpringRsocketPracticeApplication {

    public static final Logger log = LoggerFactory.getLogger(SpringRsocketPracticeApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringRsocketPracticeApplication.class, args);
    }
}


@Component
class Producer implements Ordered, ApplicationListener<ApplicationReadyEvent> {

    public static final Logger log = LoggerFactory.getLogger(SpringRsocketPracticeApplication.class);

/*
    Flux<String> notifications() {
        return Flux.fromStream(Stream.generate(new Supplier<String>() {
            @Override
            public String get() {
                return "Hello 0" + Instant.now().toString();
            }
        }));
    }
*/


    Flux<String> notifications(String name) {
        return Flux
                .fromStream(Stream.generate(() -> "Hello " + name + "" + Instant.now().toString()))
                .delayElements(Duration.ofSeconds(1));
    }


    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        SocketAcceptor socketAcceptor = (connectionSetupPayload, sender) -> {
            AbstractRSocket abstractRSocket = new AbstractRSocket() {
                @Override
                public Flux<Payload> requestStream(Payload payload) {
                    String name = payload.getDataUtf8();
                    return notifications(name)
                            .map(DefaultPayload::create);
                }
            };
            return Mono.just(abstractRSocket);
        };
        TcpServerTransport transport = TcpServerTransport.create(7000);
//        WebsocketServerTransport websocketServerTransport = WebsocketServerTransport.create(7002);

        RSocketFactory
                .receive()
                .acceptor(socketAcceptor)
                .transport(transport)
                .start()
                .block();


    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}


@Component
class Consumer implements Ordered, ApplicationListener<ApplicationReadyEvent> {

    public static final Logger log = LoggerFactory.getLogger(SpringRsocketPracticeApplication.class);

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        RSocketFactory
                .connect()
                .transport(TcpClientTransport.create(7000))
                .start()
                .flatMapMany(sender ->
                        sender.requestStream(DefaultPayload.create("Spring Tips"))
                                .map(Payload::getDataUtf8))
                .subscribe(result -> log.info("Processing new resut :::" + result));
    }
}