package keyValueStore.keyValue.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteAheadlog {


    private String fileName = null;
    private BufferedWriter bufferedWriter = null;
    private FileWriter fileWrite = null;

    public WriteAheadlog(String fileNameIn) {
        fileName = fileNameIn;
        File file = new File(fileName);
        File subdirectory = file.getParentFile();
        if (subdirectory != null) {
            if (!subdirectory.exists() && !subdirectory.mkdir()) {
                System.err.println(" failed write ");
                System.exit(0);
            }
        }
    }

    public void writeToFile(String lineIn) {
        try {
            fileWrite = new FileWriter(fileName, true);
            bufferedWriter = new BufferedWriter(fileWrite);
            bufferedWriter.write(lineIn);
            bufferedWriter.newLine();
            bufferedWriter.flush();
            closeResources();
        } catch (IOException exception) {
            System.err.println("Error write failed");
            System.exit(0);
        }
    }

    public void closeResources() {
        try {
            if (bufferedWriter != null)
                bufferedWriter.close();
        } catch (IOException exception) {
            System.err.println("close failed");
            System.exit(0);
        }
    }
}

