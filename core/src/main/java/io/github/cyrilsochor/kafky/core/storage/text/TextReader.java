package io.github.cyrilsochor.kafky.core.storage.text;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cyrilsochor.kafky.core.serde.Serdes;
import io.github.cyrilsochor.kafky.core.storage.model.Message;
import io.github.cyrilsochor.kafky.core.util.Producer;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;

public class TextReader implements Producer<Message> {

    protected final Path path;
    protected MappingIterator<Message> iterator;

    public TextReader(Path path) {
        this.path = path;
    }

    @Override
    public void init() throws Exception {
        final Reader reader = Files.newBufferedReader(path);
        final ObjectMapper mapper = Serdes.getDefaultObjectMapper();
        final JsonParser parser = mapper.createParser(reader);
        iterator = mapper
                .readValues(parser, new TypeReference<Message>() {
                });
    }

    @Override
    public Message produce() throws Exception {
        if (iterator.hasNext()) {
            return iterator.next();
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
