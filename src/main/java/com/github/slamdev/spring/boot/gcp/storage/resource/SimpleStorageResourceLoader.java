package com.github.slamdev.spring.boot.gcp.storage.resource;

import com.google.cloud.storage.Storage;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.ClassUtils;

public class SimpleStorageResourceLoader implements ResourceLoader, InitializingBean {

    private final Storage storage;
    private final ResourceLoader delegate;

    public SimpleStorageResourceLoader(Storage storage, ResourceLoader delegate) {
        this.storage = storage;
        this.delegate = delegate;
    }

    public SimpleStorageResourceLoader(Storage storage, ClassLoader classLoader) {
        this.storage = storage;
        this.delegate = new DefaultResourceLoader(classLoader);
    }

    public SimpleStorageResourceLoader(Storage storage) {
        this(storage, ClassUtils.getDefaultClassLoader());
    }

    @Override
    public Resource getResource(String location) {
        if (NameUtils.isStorageResource(location)) {
            return new StorageResource(this.storage, NameUtils.getBucketNameFromLocation(location),
                    NameUtils.getObjectNameFromLocation(location));
        }
        return this.delegate.getResource(location);
    }

    @SuppressWarnings("PMD.UseProperClassLoader")
    @Override
    public ClassLoader getClassLoader() {
        return this.delegate.getClassLoader();
    }

    @Override
    public void afterPropertiesSet() {
        // no-op
    }
}
