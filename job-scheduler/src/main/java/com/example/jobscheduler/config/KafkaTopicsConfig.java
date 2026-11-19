package com.example.jobscheduler.config;

import com.example.jobscheduler.common.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic highTopic() {
        return TopicBuilder.name(Topics.HIGH).partitions(24).replicas(1).build();
    }

    @Bean
    NewTopic normalTopic() {
        return TopicBuilder.name(Topics.NORMAL).partitions(24).replicas(1).build();
    }

    @Bean
    NewTopic lowTopic() {
        return TopicBuilder.name(Topics.LOW).partitions(24).replicas(1).build();
    }

    @Bean
    NewTopic dlqTopic() {
        return TopicBuilder.name(Topics.DLQ).partitions(1).replicas(1).build();
    }
}
