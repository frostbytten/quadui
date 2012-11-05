package org.agmip.ui.quadui;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.agmip.core.types.TranslatorInput;
import org.agmip.dome.Engine;
import org.agmip.translators.csv.DomeInput;
import org.agmip.util.MapUtil;
import org.apache.pivot.util.concurrent.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplyDomeTask extends Task<HashMap> {
    private static Logger log = LoggerFactory.getLogger(ApplyDomeTask.class);
    private HashMap m;
    private String file;

    public ApplyDomeTask(String fileName, HashMap m) {
        this.m = m;
        this.file = fileName;
    }

    @Override
    public HashMap<String, Object> execute() {
        DomeInput translator = new DomeInput();
        HashMap<String, Object> output = new HashMap<String, Object>();
        ArrayList<HashMap<String, String>> rules = null;
        ArrayList<HashMap<String, String>> info = null;
        HashMap<String, ArrayList<HashMap<String, String>>> dome;
        Engine domeEngine;
        // Load the dome
        try {
            dome = (HashMap<String, ArrayList<HashMap<String, String>>>) translator.readFile(file);
        } catch (Exception ex) {
            output.put("errors", ex.getMessage());
            return output;
        }

        rules = dome.get("rules");
        log.error("DOME Shell: {}", dome);
        if (rules != null) {
            domeEngine = new Engine(rules);
        } else {
            return m;
        }


        // Flatten the data and apply the dome.
        ArrayList<HashMap<String, Object>> flattenedData = MapUtil.flatPack(m);
        for (HashMap<String, Object> entry : flattenedData) {
            domeEngine.apply(entry);
        }

        output.put("domeoutput", MapUtil.bundle(flattenedData));
        output.put("domeinfo", info);
        return output;
    } 
}