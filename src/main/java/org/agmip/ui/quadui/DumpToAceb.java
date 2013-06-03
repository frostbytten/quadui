package org.agmip.ui.quadui;

import com.rits.cloning.Cloner;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.agmip.ace.io.AceGenerator;

import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;


import static org.agmip.util.JSONAdapter.toJSON;

public class DumpToAceb extends Task<String> {

    private HashMap data;
    private String fileName, directoryName;

    public DumpToAceb(String file, String dirName, HashMap data) {
        this.fileName = file;
        this.directoryName = dirName;
        Cloner cloner = new Cloner();
        this.data = cloner.deepClone(data);
    }

    @Override
    public String execute() throws TaskExecutionException {
        File file = new File(fileName);
        String[] base = file.getName().split("\\.(?=[^\\.]+$)");
        String outputAceb = directoryName + "/" + base[0] + ".aceb";
        file = new File(outputAceb);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            AceGenerator.generateACEB(file, toJSON(data));
        } catch (IOException ex) {
            throw new TaskExecutionException(ex);
        }
        return null;
    }
}
