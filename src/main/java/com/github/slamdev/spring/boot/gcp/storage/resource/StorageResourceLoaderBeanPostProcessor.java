package com.github.slamdev.spring.boot.gcp.storage.resource;

import com.google.cloud.storage.Storage;
import org.springframework.beans.BeanInstantiationException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.Ordered;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;

/**
 * {@link BeanPostProcessor} and {@link BeanFactoryPostProcessor} implementation that allows classes to receive
 * a specialized {@link ResourceLoader} that can handle gcs resources with the {@link ResourceLoaderAware} interface
 * or through injecting the resource loader.
 */
public class StorageResourceLoaderBeanPostProcessor implements BeanPostProcessor, BeanFactoryPostProcessor, Ordered, ResourceLoaderAware {

    private final Storage storage;
    private ResourceLoader resourceLoader;

    public StorageResourceLoaderBeanPostProcessor(Storage storage) {
        this.storage = storage;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        if (bean instanceof ResourceLoaderAware) {
            ((ResourceLoaderAware) bean).setResourceLoader(this.resourceLoader);
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName){
        return bean;
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        SimpleStorageResourceLoader simpleStorageResourceLoader = new SimpleStorageResourceLoader(this.storage, this.resourceLoader);
        try {
            simpleStorageResourceLoader.afterPropertiesSet();
        } catch (Exception e) {
            throw new BeanInstantiationException(SimpleStorageResourceLoader.class, "Error instantiating class", e);
        }

        this.resourceLoader = new PathMatchingSimpleStorageResourcePatternResolver(this.storage,
                simpleStorageResourceLoader, (ResourcePatternResolver) this.resourceLoader);


        beanFactory.registerResolvableDependency(ResourceLoader.class, this.resourceLoader);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }
}
