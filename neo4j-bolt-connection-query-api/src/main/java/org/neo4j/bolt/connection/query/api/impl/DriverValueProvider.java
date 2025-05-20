package org.neo4j.bolt.connection.query.api.impl;

import com.fasterxml.jackson.jr.ob.api.ReaderWriterProvider;
import com.fasterxml.jackson.jr.ob.api.ValueReader;
import com.fasterxml.jackson.jr.ob.api.ValueWriter;
import com.fasterxml.jackson.jr.ob.impl.JSONReader;
import com.fasterxml.jackson.jr.ob.impl.JSONWriter;

/**
 * @author Gerrit Meier
 */
public class DriverValueProvider extends ReaderWriterProvider {

    @Override
    public ValueReader findValueReader(JSONReader readContext, Class<?> type) {
        return super.findValueReader(readContext, type);
    }

    @Override
    public ValueWriter findValueWriter(JSONWriter writeContext, Class<?> type) {
        return super.findValueWriter(writeContext, type);
    }
}
