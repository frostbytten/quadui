package org.agmip.ui.quadui;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.agmip.core.types.TranslatorOutput;

/**
 * Temporal solution for model specific data distribution purpose
 *
 * @author Meng Zhang
 */
public class ModelFileDumperOutput implements TranslatorOutput {

    @Override
    public void writeFile(String outputDirectory, Map data) throws IOException {
        Set<Entry> files = data.entrySet();
        for (Entry file : files) {
            String fileName = (String) file.getKey();
            char[] content = (char[]) file.getValue();
            FileWriter fw = new FileWriter(outputDirectory + File.separator + fileName);
            fw.write(content);
            fw.flush();
            fw.close();
        }
    }
}
