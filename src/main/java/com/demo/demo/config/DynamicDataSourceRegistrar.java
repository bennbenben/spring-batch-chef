package com.demo.demo.config;

import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

public class DynamicDataSourceRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        
        // 1. Bind the YAML map 'app.datasource' to a Map<String, DataSourceProperties>
        Map<String, DataSourceProperties> datasources = Binder.get(environment)
                .bind("app.datasource", Bindable.mapOf(String.class, DataSourceProperties.class))
                .orElse(Collections.emptyMap());

        // 2. Loop through every database defined in the YAML
        for (Map.Entry<String, DataSourceProperties> entry : datasources.entrySet()) {
            String beanName = entry.getKey(); // e.g., "primary", "ledgerDb", "archiveDb"
            DataSourceProperties properties = entry.getValue();

            // 3. Create the Bean Definition
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(DataSource.class);
            
            // This is a Spring Boot 3 trick: We provide a Supplier that builds the DataSource when requested
            beanDefinition.setInstanceSupplier(() -> properties.initializeDataSourceBuilder().build());

            // 4. Spring Batch REQUIRES exactly one DataSource to be marked as @Primary for its metadata.
            // We enforce a rule: if the YAML key is "primary", mark it as the primary bean.
            if ("primary".equalsIgnoreCase(beanName)) {
                beanDefinition.setPrimary(true);
            }

            // 5. Register the bean into the Application Context
            registry.registerBeanDefinition(beanName, beanDefinition);
            System.out.println("Spring batch chef: Dynamically registered DataSource bean = " + beanName);
        }
    }
}