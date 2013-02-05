package org.agmip.ui.quadui;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import java.util.HashMap;


import org.agmip.core.types.TranslatorOutput;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;

import org.agmip.translators.apsim.ApsimOutput;
import org.agmip.translators.dssat.DssatControllerOutput;
import org.agmip.translators.dssat.DssatWeatherOutput;
import org.agmip.util.AcmoUtil;
        

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agmip.util.JSONAdapter.toJSON;

public class TranslateToTask extends Task<String> {

    private HashMap data;
    private ArrayList<String> translateList;
    private ArrayList<String> weatherList, soilList;
    private String destDirectory;
    private static Logger LOG = LoggerFactory.getLogger(TranslateToTask.class);

    public TranslateToTask(ArrayList<String> translateList, HashMap data, String destDirectory) {
        this.data = data;
        this.destDirectory = destDirectory;
        this.translateList = new ArrayList<String>();
        this.weatherList = new ArrayList<String>();
        this.soilList = new ArrayList<String>();
        for (String trType : translateList) {
            if (!trType.equals("JSON")) {
                this.translateList.add(trType);
            }
        }
        if (data.containsKey("weathers")) {
            for (HashMap<String, Object> stations : (ArrayList<HashMap>) data.get("weathers")) {
                weatherList.add((String) stations.get("wst_id"));
            }
        }

        if (data.containsKey("soils")) {
            for (HashMap<String, Object> soils : (ArrayList<HashMap>) data.get("soils")) {
                soilList.add((String) soils.get("soil_id"));
            }
        }
    }

    @Override
        public String execute() throws TaskExecutionException {
            ExecutorService executor = Executors.newFixedThreadPool(64);
            try {
                for (String tr : translateList) {
                    // Generate the ACMO here (pre-generation) so we know what
                    // we should get out of everything.
                    AcmoUtil.writeAcmo(destDirectory+File.separator+tr.toUpperCase(), data, tr.toLowerCase());
                    if (data.size() == 1 && data.containsKey("weather")) {
                        LOG.info("Running in weather only mode");
                        submitTask(executor,tr,data,true);
                    } else {
                        submitTask(executor, tr, data, false);
                    }
                }
                executor.shutdown();
                while (!executor.isTerminated()) {
                }
                executor = null;
                //this.data = null;
            } catch (Exception ex) {
                throw new TaskExecutionException(ex);
            }
            return null;
        }

    /**
     * Submit a task to an executor to start translation.
     * 
     * @param executor The <code>ExecutorService</code> to execute this thread on.
     * @param trType The model name to translate to (used to instantiate the
     *                proper <code>TranslatorOutput</code> 
     * @param data The data to translate
     */
    private void submitTask(ExecutorService executor, String trType, HashMap<String, Object> data, boolean wthOnly) {
        TranslatorOutput translator = null;
        String destination = "";
        if (trType.equals("DSSAT")) {
            if (wthOnly) {
                translator = new DssatWeatherOutput();
            } else { 
                translator = new DssatControllerOutput();
            }
        } else if (trType.equals("APSIM")) {
            translator = new ApsimOutput();
        }
        destination = destDirectory + File.separator + trType;
        LOG.debug("Translating with :"+translator.getClass().getName());
        Runnable thread = new TranslateRunner(translator, data, destination);
        executor.execute(thread);
    }
}
