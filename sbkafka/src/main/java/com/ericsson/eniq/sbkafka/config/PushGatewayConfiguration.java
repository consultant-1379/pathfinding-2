/*package com.ericsson.eniq.sbkafka.config;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.actuate.autoconfigure.metrics.export.prometheus.PrometheusProperties;
import org.springframework.boot.actuate.metrics.export.prometheus.PrometheusPushGatewayManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(PrometheusProperties.class)
public class PushGatewayConfiguration {
	
	@Value("${management.metrics.export.prometheus.pushgateway.base-url}")
	private String url;

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags(@Value("${spring.profiles.active:default}") String activeEnvProfile) {
        return registry -> registry.config()
                .commonTags(
                        "env", activeEnvProfile
                );
    }
    
    @Bean
    @ConditionalOnMissingBean
    public CollectorRegistry collectorRegistry() {
        return new CollectorRegistry(true);
    }

    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }


    @ConditionalOnProperty(prefix = "management.metrics.export.prometheus.pushgateway", value = "enabled", havingValue = "true")
    @Bean
    @Primary
    public PrometheusPushGatewayManager prometheusPushGatewayManager(CollectorRegistry collectorRegistry,
                                                                     PrometheusProperties prometheusProperties,
                                                                     Environment environment) throws MalformedURLException {
        PrometheusProperties.Pushgateway properties = prometheusProperties.getPushgateway();
        Duration pushRate = properties.getPushRate();
        String job = getJob(properties, environment);
        Map<String, String> groupingKey = properties.getGroupingKey();
        PrometheusPushGatewayManager.ShutdownOperation shutdownOperation = properties.getShutdownOperation();
        return new PrometheusPushGatewayManager(this.getPushGateway(), collectorRegistry, pushRate, job, groupingKey, shutdownOperation);
    }

    @Bean
    public PushGateway getPushGateway() throws MalformedURLException {
        return new PushGateway(new URL(url));
        
    }

    private static String getJob(PrometheusProperties.Pushgateway properties, Environment environment) {
        String job = properties.getJob();
        job = job != null ? job : environment.getProperty("spring.application.name");
        return job != null ? job : "demo-service";
    }
}*/