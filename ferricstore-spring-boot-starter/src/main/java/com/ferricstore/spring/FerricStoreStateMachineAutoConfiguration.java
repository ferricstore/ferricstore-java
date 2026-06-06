package com.ferricstore.spring;

import com.ferricstore.spring.statemachine.FerricFlowStateMachine;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.statemachine.config.StateMachineFactory;

@AutoConfiguration(after = FerricStoreAutoConfiguration.class)
@ConditionalOnClass({FerricFlowStateMachine.class, StateMachineFactory.class})
public class FerricStoreStateMachineAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(StateMachineFactory.class)
    FerricFlowStateMachine ferricFlowStateMachine(StateMachineFactory<String, String> factory) {
        return FerricFlowStateMachine.builder(factory).build();
    }
}
