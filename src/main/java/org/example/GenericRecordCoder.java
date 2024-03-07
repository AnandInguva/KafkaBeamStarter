package org.example;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.cache.Cache;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;

/**
 * This coder is used when the schema of the incoming GenericRecord can only be known
 * at runtime.
 */
public class GenericRecordCoder extends AtomicCoder<GenericRecord> {
    private static final Integer MAX_CACHE_SIZE = 100;

    public static GenericRecordCoder of() {
        return new GenericRecordCoder();
    }

    private static final Cache<String, AvroCoder<GenericRecord>> avroCoderCache =
            CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build();

    @Override
    public void encode(GenericRecord value, OutputStream outStream) throws IOException {
        String schemaString = value.getSchema().toString();
        StringUtf8Coder.of().encode(schemaString, outStream);
        AvroCoder<GenericRecord> coder = getAvroCoder(value.getSchema().toString());
        coder.encode(value, outStream);
    }

    @Override
    public GenericRecord decode(InputStream inStream) throws IOException {
        String schemaString = StringUtf8Coder.of().decode(inStream);
        AvroCoder<GenericRecord> coder = getAvroCoder(schemaString);
        return coder.decode(inStream);
    }



    private AvroCoder<GenericRecord> getAvroCoder(String schemaString) {
       try {
           return avroCoderCache.get(schemaString, () -> AvroCoder.of(new Schema.Parser().parse(schemaString)));
       } catch (ExecutionException e) {
           throw new AssertionError("Impoosbile");
       }
    }
}
