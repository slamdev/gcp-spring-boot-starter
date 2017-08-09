package com.github.slamdev.spring.boot.gcp.storage.resource;

import com.google.cloud.storage.*;
import lombok.Getter;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.WritableResource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.function.Supplier;

import static com.github.slamdev.spring.boot.gcp.storage.resource.LazySupplier.lazily;

public class StorageResource extends AbstractResource implements WritableResource {

    @Getter
    private final String bucketName;
    @Getter
    private final String objectName;
    private final Storage storage;
    private final Supplier<Blob> objectMetadata = lazily(this::getObjectMetadata);

    StorageResource(Storage storage, String bucketName, String objectName) {
        this.storage = storage;
        this.bucketName = bucketName;
        this.objectName = objectName;
    }

    @Override
    public String getDescription() {
        StringBuilder builder = new StringBuilder("Google Cloud Storage resource [bucketName='");
        builder.append(bucketName);
        builder.append("' and object='");
        builder.append(objectName);
        builder.append("']");
        return builder.toString();
    }

    @Override
    public InputStream getInputStream() {
        BlobId id = BlobId.of(bucketName, objectName);
        return Channels.newInputStream(storage.reader(id));
    }

    @Override
    public boolean exists() {
        return objectMetadata.get() != null;
    }

    @Override
    public long contentLength() throws FileNotFoundException {
        return getRequiredObjectMetadata().getSize();
    }

    @Override
    public long lastModified() throws FileNotFoundException {
        return getRequiredObjectMetadata().getUpdateTime();
    }

    @Override
    public String getFilename() {
        return objectName;
    }

    @Override
    public URL getURL() throws FileNotFoundException, MalformedURLException {
        return new URL(getRequiredObjectMetadata().getSelfLink());
    }

    @Override
    public File getFile() {
        throw new UnsupportedOperationException("Google Cloud Storage resource can not be resolved to java.io.File "
                + "objects. Use getInputStream() to retrieve the contents of the object!");
    }

    private Blob getRequiredObjectMetadata() throws FileNotFoundException {
        Blob metadata = objectMetadata.get();
        if (metadata == null) {
            StringBuilder builder = new StringBuilder();
            builder.append("Resource with bucket='");
            builder.append(bucketName);
            builder.append("' and objectName='");
            builder.append(objectName);
            builder.append("' not found!");
            throw new FileNotFoundException(builder.toString());
        }
        return metadata;
    }

    @Override
    public boolean isWritable() {
        return true;
    }

    @Override
    public OutputStream getOutputStream() {
        BlobId id = BlobId.of(bucketName, objectName);
        BlobInfo info = BlobInfo.newBuilder(id).build();
        return Channels.newOutputStream(storage.writer(info));
    }

    @Override
    public StorageResource createRelative(String relativePath) {
        String relativeKey = objectName + "/" + relativePath;
        return new StorageResource(storage, bucketName, relativeKey);
    }

    public boolean delete() {
        return storage.delete(BlobId.of(bucketName, objectName));
    }

    private Blob getObjectMetadata() {
        try {
            BlobId id = BlobId.of(bucketName, objectName);
            return storage.get(id);
        } catch (StorageException e) {
            // Catch 404 (object not found) and 301 (bucket not found, moved permanently)
            if (e.getCode() == 404 || e.getCode() == 301) {
                return null;
            } else {
                throw e;
            }
        }
    }
}
