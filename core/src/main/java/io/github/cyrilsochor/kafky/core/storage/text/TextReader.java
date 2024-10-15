package io.github.cyrilsochor.kafky.core.storage.text;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cyrilsochor.kafky.api.job.Producer;
import io.github.cyrilsochor.kafky.core.serde.Serdes;
import io.github.cyrilsochor.kafky.core.storage.model.Message;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;

public class TextReader implements Producer<Message> {

    protected final Reader reader;
    protected MappingIterator<Message> iterator;

    public TextReader(final Reader reader) {
        this.reader = reader;
    }

    public TextReader(Path path) throws IOException {
        this(Files.newBufferedReader(path));
    }

    @Override
    public void init() throws Exception {
        final ObjectMapper mapper = Serdes.getDefaultObjectMapper();
        final JsonParser parser = mapper.createParser(reader);
        iterator = mapper.readValues(parser, Message.class);
    }

    @Override
    public Message produce() throws Exception {
        if (iterator.hasNextValue()) {
            return iterator.nextValue();
        } else {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (iterator != null) {
            iterator.close();
        }
    }

}
