package com.bootstudy.rsocketserver;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Controller
public class RSocketService {
    // reactive repository : 몽고 DB
    private final ItemRepository itemRepository;
    // 최근 1개의 메시지만 보내기 위한 FluxProcessor
    private final EmitterProcessor itemProcessor;
    // itemProcessor 에 item 을 추가하기 위한 진입점
    private final FluxSink<Item> itemSink;

    public RSocketService(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
        this.itemProcessor = EmitterProcessor.create();
        this.itemSink = this.itemProcessor.sink();
    }

    // R 소켓 라우팅 규칙
    @MessageMapping("newItems.request-response")
    public Mono<Item> processNewItemsViaRSocketRequestResponse(Item item) {
        return this.itemRepository.save(item) // 데이터 저장
                .doOnNext(savedItem -> this.itemSink.next(savedItem)); // 저장된 데이터를 FluxProcessor 로 전달
    }

    @MessageMapping("newItems.request-stream")
    public Flux<Item> findItemsViaRSocketRequestStream() {
        return this.itemRepository.findAll() // DB 조회후
                .doOnNext(this.itemSink::next); // FluxProcessor 로 전달
    }

    @MessageMapping("newItems.fire-and-forget")
    public Mono<Void> processNewItemsViaRSocketFireAndForget(Item item) {
        return this.itemRepository.save(item) // 데이터 저장
                .doOnNext(savedItem -> this.itemSink.next(savedItem))
                .then(); // 데이터를 반환하지 않기 때문에 then 호출
    }

    @MessageMapping("newItems.monitor")
    public Flux<Item> monitorNewItems() {
        return this.itemProcessor; // 복제된 Flux 인 EmitterProcessor
    }
}
