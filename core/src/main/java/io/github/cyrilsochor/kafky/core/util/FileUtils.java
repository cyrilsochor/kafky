package io.github.cyrilsochor.kafky.core.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class FileUtils {

    private FileUtils() {
        // no instance
    }

    public static BufferedWriter createWriter(Path path, OpenOption... options) throws IOException {
        final Path dir = path.getParent();
        Files.createDirectories(dir);
        return Files.newBufferedWriter(path, options);
    }

}
