package org.agmip.ui.quadui;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import java.util.LinkedHashMap;


import org.agmip.core.types.TranslatorOutput;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;

import org.agmip.translators.apsim.ApsimOutput;
import org.agmip.translators.dssat.DssatControllerOutput;
import org.agmip.translators.dssat.DssatWeatherOutput;
        

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agmip.util.JSONAdapter.toJSON;

public class TranslateToTask extends Task<String> {

    private LinkedHashMap data;
    private ArrayList<String> translateList;
    private ArrayList<String> weatherList, soilList;
    private String destDirectory;
    private static Logger LOG = LoggerFactory.getLogger(TranslateToTask.class);

    public TranslateToTask(ArrayList<String> translateList, LinkedHashMap data, String destDirectory) {
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
            for (LinkedHashMap<String, Object> stations : (ArrayList<LinkedHashMap>) data.get("weathers")) {
                weatherList.add((String) stations.get("wst_id"));
            }
        }

        if (data.containsKey("soils")) {
            for (LinkedHashMap<String, Object> soils : (ArrayList<LinkedHashMap>) data.get("soils")) {
                soilList.add((String) soils.get("soil_id"));
            }
        }
    }

    @Override
        public String execute() throws TaskExecutionException {
            ExecutorService executor = Executors.newFixedThreadPool(64);
            try {
                for (String tr : translateList) {
                    if (tr.equals("DSSAT")) {
                        if (data.size() == 1 && data.containsKey("weather")) {
                            LOG.info("Running in weather only mode");
                            submitTask(executor,tr,data,true);
                        } else {
                            submitTask(executor, tr, data, false);
                        }
                    } else {
                        // Handle translators that do not support the multi-experiment
                        // format.
                        if (data.containsKey("experiments")) {
                            for (LinkedHashMap<String, Object> experiment : (ArrayList<LinkedHashMap>) data.get("experiments")) {
                                LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>(experiment);
                                int wKey, sKey;
                                if (temp.containsKey("wst_id")) {
                                    if ((wKey = weatherList.indexOf((String) temp.get("wst_id"))) != -1) {
                                        temp.put("weather", ((ArrayList<LinkedHashMap<String, Object>>) data.get("weathers")).get(wKey));
                                    }
                                }
                                if (temp.containsKey("soil_id")) {
                                    if ((sKey = soilList.indexOf((String) temp.get("soil_id"))) != -1) {
                                        temp.put("soil", ((ArrayList<LinkedHashMap<String, Object>>) data.get("soils")).get(sKey));
                                    }
                                }
                                LOG.info("JSON of temp:"+toJSON(temp));
                                // need to re-implement properly for threading apsim
                                //submitTask(executor, tr, temp);
                                if(tr.equals("APSIM")) {
                                    ApsimOutput translator = new ApsimOutput();
                                    translator.writeFile(destDirectory+File.separator+"APSIM", temp);
                                }
                            }
                        } else {
                            boolean wthOnly = false;
                            if ( data.size() == 1 && data.containsKey("weather") ) {
                                wthOnly = true;
                            }
                            //Assume this is a single complete experiment
                            submitTask(executor, tr, data, wthOnly);
                        }
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
    private void submitTask(ExecutorService executor, String trType, LinkedHashMap<String, Object> data, boolean wthOnly) {
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
        LOG.info("Translating with :"+translator.getClass().getName());
        Runnable thread = new TranslateRunner(translator, data, destination);
        executor.execute(thread);
    }
}
