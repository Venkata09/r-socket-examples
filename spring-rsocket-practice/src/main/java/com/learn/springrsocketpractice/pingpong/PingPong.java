package com.learn.springrsocketpractice.pingpong;

import io.rsocket.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@SpringBootApplication
public class PingPong {


    static String reply(String input) {
        if (input.equalsIgnoreCase("ping")) return "pong";
        if (input.equalsIgnoreCase("pong")) return "ping";
        throw new IllegalArgumentException(" Incoming value must be either ping or pong!!!");
    }

    public static void main(String[] args) {
        SpringApplication.run(PingPong.class, args);
    }
}

@Log4j2
@Component
class Ping implements ApplicationListener<ApplicationReadyEvent>, Ordered {

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("Starting ::::: " + this.getClass().getName());

        RSocketFactory
                .connect()
                .transport(TcpClientTransport.create(7000))
                .start().flatMapMany(socket ->
                socket.requestChannel(Flux.interval(Duration.ofSeconds(1)).map(i -> DefaultPayload.create("ping")))
                        .map(payload -> payload.getDataUtf8())
                        .doOnNext(eachString -> log.info("received :::" + eachString + getClass().getName()))
                        .take(10)
                        .doFinally(signal -> socket.dispose())
        ).then()
                .block();

    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}


@Log4j2
@Component
class Pong implements SocketAcceptor, Ordered, ApplicationListener<ApplicationReadyEvent> {

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        RSocketFactory
                .receive()
                .acceptor(this)
                .transport(TcpServerTransport.create(7000))
                .start()
                .subscribe();
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket rSocket) {
        AbstractRSocket rs = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return Flux
                        .from(payloads)
                        .map(Payload::getDataUtf8)
                        .doOnNext(eachEntry -> log.info(" received :::" + eachEntry + "  in " + getClass().getName()))
                        .map(PingPong::reply)
                        .map(DefaultPayload::create);
            }
        };


        return Mono.just(rs);
    }
}