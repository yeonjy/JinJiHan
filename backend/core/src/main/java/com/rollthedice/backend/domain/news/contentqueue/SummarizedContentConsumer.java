package com.rollthedice.backend.domain.news.contentqueue;

import com.rabbitmq.client.Channel;
import com.rollthedice.backend.domain.news.dto.ContentMessageDto;
import com.rollthedice.backend.domain.news.service.NewsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Component
public class SummarizedContentConsumer {
    private static final int MAX_RETRIES = 3;
    private final NewsService newsService;

    @RabbitListener(queues = "${rabbitmq.store.queue.name}")
    public void receiveMessage(ContentMessageDto messageDto, Channel channel, Message message) throws IOException {
        try {
            log.info("Received summarized news message id: {}", messageDto.getId());
            newsService.updateSummarizedNews(messageDto);
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (Exception e) {
            log.error("Error while processing message. id: {}", messageDto.getId(), e);
            handleRetry(channel, message);
        }
    }

    private void handleRetry(Channel channel, Message message) throws IOException {
        Map<String, Object> headers = message.getMessageProperties().getHeaders();

        long retryCount = 0;
        if (headers.get("x-retry") != null) {
            retryCount = ((List<Map<String, Object>>) headers.get("x-retry")).size();
        }

        if (retryCount < MAX_RETRIES) {
            log.error("Exceeded max retries for message.");
            channel.basicReject(message.getMessageProperties().getDeliveryTag(),true);
        } else {
            channel.basicReject(message.getMessageProperties().getDeliveryTag(),false);
        }
    }
}
