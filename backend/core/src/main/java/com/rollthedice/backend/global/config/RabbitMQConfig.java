package com.rollthedice.backend.global.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class RabbitMQConfig {

    @Value("${spring.rabbitmq.host}")
    private String rabbitmqHost;

    @Value("${spring.rabbitmq.port}")
    private int rabbitmqPort;

    @Value("${spring.rabbitmq.username}")
    private String rabbitmqUsername;

    @Value("${spring.rabbitmq.password}")
    private String rabbitmqPassword;

    @Value("${rabbitmq.summary.queue.name}")
    private String summaryQueueName;

    @Value("${rabbitmq.store.queue.name}")
    private String storeQueueName;

    @Value("${rabbitmq.store.dlq.queue.name}")
    private String storeDlqQueueName;

    @Value("${rabbitmq.summary.exchange.name}")
    private String summaryExchangeName;

    @Value(("${rabbitmq.store.exchange.name}"))
    private String storeExchangeName;

    @Value("${rabbitmq.summary.routing.key}")
    private String summaryRoutingKey;

    @Value("${rabbitmq.store.routing.key}")
    private String storeRoutingKey;

    @Value("${rabbitmq.store.dlq.routing.key}")
    private String storeDlqRoutingKey;

    @Bean
    public Queue summaryQueue() {
        return QueueBuilder.durable(summaryQueueName).build();
    }

    @Bean
    public Queue storeQueue() {
        return QueueBuilder.durable(storeQueueName)
                .withArgument("x-dead-letter-exchange", storeExchangeName)
                .withArgument("x-dead-letter-routing-key", storeDlqRoutingKey)
                .withArgument("x-message-ttl", 60000)
                .build();
    }

    @Bean
    public Queue storeDeadLetterQueue() {
        return QueueBuilder.durable(storeDlqQueueName).build();
    }

    @Bean
    public DirectExchange summaryExchange() {
        return new DirectExchange(summaryExchangeName);
    }

    @Bean
    public DirectExchange storeExchange() {
        return new DirectExchange(storeExchangeName);
    }

    @Bean
    public Binding summaryBinding(Queue summaryQueue, DirectExchange summaryExchange) {
        return BindingBuilder.bind(summaryQueue).to(summaryExchange).with(summaryRoutingKey);
    }

    @Bean
    public Binding storeBinding(Queue storeQueue, DirectExchange storeExchange) {
        return BindingBuilder.bind(storeQueue).to(storeExchange).with(storeRoutingKey);
    }

    @Bean
    public Binding storeDeadLetterBinding(Queue storeDeadLetterQueue, DirectExchange storeExchange) {
        return BindingBuilder.bind(storeDeadLetterQueue).to(storeExchange).with(storeDlqRoutingKey);
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost(rabbitmqHost);
        connectionFactory.setPort(rabbitmqPort);
        connectionFactory.setUsername(rabbitmqUsername);
        connectionFactory.setPassword(rabbitmqPassword);
        return connectionFactory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(jackson2JsonMessageConverter());
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                log.info("Message successfully delivered to the broker.");
            } else {
                log.error("Message delivery failed: {}", cause);
            }
        });
        return rabbitTemplate;
    }

    @Bean
    public MessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
