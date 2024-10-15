package io.github.cyrilsochor.kafky.core.storage.text;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cyrilsochor.kafky.api.job.Consumer;
import io.github.cyrilsochor.kafky.core.serde.Serdes;
import io.github.cyrilsochor.kafky.core.storage.model.Message;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

public class TextWriter implements Consumer<Message> {

    protected final Path path;
    protected Writer writer;
    protected ObjectMapper mapper;

    public TextWriter(Path path) {
        this.path = path;
    }

    @Override
    public void init() throws Exception {
        writer = Files.newBufferedWriter(path);
        mapper = Serdes.getDefaultObjectMapper();
    }

    @Override
    public void consume(final Message message) throws Exception {
        final String messageString = mapper.writeValueAsString(message);
        writer.append(messageString);
        writer.flush();
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }

}
