package keyValueStore.keyValue.util;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.InvalidPathException;

public final class FileProcessor {
    /**
     * FileProcessor used to read input file
     */
    private BufferedReader reader;

    public FileProcessor(String inputFilePath)
            throws InvalidPathException, SecurityException, FileNotFoundException, IOException {

        if (!Files.exists(Paths.get(inputFilePath))) {
            throw new FileNotFoundException("invalid input file or input file in incorrect location");
        }

        reader = new BufferedReader(new FileReader(new File(inputFilePath)));
    }

    public String poll() throws IOException {
        String line;
        line = reader.readLine();
        return line;
    }

    public void close() throws IOException {
        try {
            reader.close();
        } catch (IOException e) {
            throw new IOException("failed to close file", e);
        }
    }

}