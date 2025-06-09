package io.github.cyrilsochor.kafky.core.storage.text;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.api.job.Consumer;
import io.github.cyrilsochor.kafky.core.serde.Serdes;
import io.github.cyrilsochor.kafky.core.storage.model.Message;
import io.github.cyrilsochor.kafky.core.util.FileUtils;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;

public class TextWriter implements Consumer<Message>, Component {

    protected final Writer writer;
    protected ObjectMapper mapper;

    public TextWriter(final Writer writer) {
        this.writer = writer;
    }

    public TextWriter(final Path path) throws IOException {
        this(FileUtils.createWriter(path));
    }

    @Override
    public void init() throws Exception {
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
