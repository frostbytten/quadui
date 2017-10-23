package org.agmip.ui.quadui;

import com.rits.cloning.Cloner;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.agmip.ace.AceDataset;
import org.agmip.ace.io.AceParser;


import org.agmip.core.types.TranslatorOutput;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;

import org.agmip.translators.apsim.ApsimWriter;
import org.agmip.translators.dssat.DssatControllerOutput;
import org.agmip.translators.dssat.DssatWeatherOutput;
import org.agmip.acmo.util.AcmoUtil;
import org.agmip.common.Functions;
import org.agmip.core.types.DividableOutputTranslator;
import org.agmip.translators.agmip.AgmipOutput;
import org.agmip.translators.apsim.ApsimWriterDiv;
import org.agmip.translators.cropgrownau.CropGrowNAUOutput;
import org.agmip.translators.stics.SticsOutput;
import org.agmip.translators.wofost.WofostOutputController;
import org.agmip.util.JSONAdapter;

// import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TranslateToTask extends Task<String> {

    private final HashMap data;
    private final AceDataset aceData;
    private final ArrayList<String> translateList;
    private boolean hasSarraH33 = false;
    private final ArrayList<String> weatherList, soilList;
    private final String destDirectory;
    private final boolean compress;
    private boolean isApsimDiv = false;
    private int divSize = 1;
    private final HashMap<String, String> domeIdHashMap;
    private final HashMap<String, HashMap> modelSpecFiles;
    private static final Logger LOG = LoggerFactory.getLogger(TranslateToTask.class);
    private final boolean isRunCmd;
    

    public TranslateToTask(ArrayList<String> translateList, HashMap data, String destDirectory, boolean compress, HashMap<String, String> domeIdHashMap, HashMap<String, HashMap> modelSpecFiles) {
        this.data = data;
        if (translateList.contains("AgMIP") || translateList.contains("SarraHV33")) {
            this.aceData = toAceDataset(data);
        } else {
            this.aceData = null;
        }
        this.destDirectory = destDirectory;
        this.translateList = new ArrayList<String>();
        this.weatherList = new ArrayList<String>();
        this.soilList = new ArrayList<String>();
        this.compress = compress;
        this.domeIdHashMap = domeIdHashMap;
        this.modelSpecFiles = modelSpecFiles;
        for (String trType : translateList) {
            if (trType.equals("SarraHV33")) {
                hasSarraH33 = true;
            } else if (trType.startsWith("APSIM_Div")) {
                this.translateList.add("APSIM");
                isApsimDiv = true;
                if (trType.startsWith("APSIM_Div_")) {
                    divSize = Integer.parseInt(trType.replaceAll("APSIM_Div_", ""));
                }
            } else if (!trType.equals("JSON")) {
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
        StackTraceElement stack[] = (new Throwable()).getStackTrace();
        if (stack.length > 1) {
            this.isRunCmd = stack[1].getClassName().endsWith(QuadCmdLine.class.getName());
        } else {
            this.isRunCmd = false;
        }
    }

    @Override
        public String execute() throws TaskExecutionException {
            ExecutorService executor = Executors.newFixedThreadPool(64);
            try {
                for (String tr : translateList) {
                    // Generate the ACMO here (pre-generation) so we know what
                    // we should get out of everything.
                    File destDir = createModelDestDirectory(destDirectory, tr);
                    AcmoUtil.writeAcmo(destDir.toString(), data, tr.toLowerCase(), domeIdHashMap);
                    // Dump the model specific files into corresponding model folder
                    if (modelSpecFiles != null) {
                        HashMap modelFiles = (HashMap) modelSpecFiles.get(tr.toLowerCase());
                        if (modelFiles != null && !modelFiles.isEmpty()) {
                            try {
                                new ModelFileDumperOutput().writeFile(destDir.toString(), modelFiles);
                            } catch (Exception e) {
                                LOG.error(Functions.getStackTrace(e));
                            }
                        }
                    }
                    if (data.size() == 1 && data.containsKey("weather")) {
                        LOG.info("Running in weather only mode");
                        submitTask(executor, tr, data, destDir, true, compress);
                    } else {
                        submitTask(executor, tr, data, destDir, false, compress);
                    }
                }
                if (hasSarraH33) {
                    LOG.info("SarraHV33 Translator Started");
                    File destDir = createModelDestDirectory(destDirectory, "SarraHV33");
                    // Dump the model specific files into corresponding model folder
                    if (modelSpecFiles != null) {
                        HashMap modelFiles = (HashMap) modelSpecFiles.get("sarrahv33");
                        if (modelFiles != null && !modelFiles.isEmpty()) {
                            try {
                                new ModelFileDumperOutput().writeFile(destDir.toString(), modelFiles);
                            } catch (Exception e) {
                                LOG.error(Functions.getStackTrace(e));
                            }
                        }
                    }
                    Runnable thread = new TranslateRunnerSarraH(aceData, destDir.toString(), "SarraHV33", compress);
                    executor.execute(thread);
                }
                executor.shutdown();
                while (!executor.isTerminated()) {
                }
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
    private void submitTask(ExecutorService executor, String trType, HashMap<String, Object> data, File path, boolean wthOnly, boolean compress) {
        Cloner cloner = new Cloner();
        data = cloner.deepClone(data);
        TranslatorOutput translator = null;
        org.agmip.ace.translators.io.TranslatorOutput newTranslator = null;
        if (trType.equals("DSSAT")) {
            if (wthOnly) {
                LOG.info("DSSAT Weather Translator Started");
                translator = new DssatWeatherOutput();
            } else { 
                LOG.info("DSSAT Translator Started");
                translator = new DssatControllerOutput();
            }
        } else if (trType.equals("APSIM")) {
            LOG.info("APSIM Translator Started");
            if (isApsimDiv) {
                translator = new ApsimWriterDiv();
            } else {
                translator = new ApsimWriter();
            }
            ((ApsimWriter) translator).setOutputCraftBat(this.isRunCmd);
        } else if (trType.equals("STICS")) {
            LOG.info("STICS Translator Started");
            translator = new SticsOutput();
        } else if (trType.equals("WOFOST")) {
            LOG.info("WOFOST Translator Started");
            translator = new WofostOutputController();
        } else if (trType.equals("CropGrow-NAU")) {
            LOG.info("CropGrow-NAU Translator Started");
            translator = new CropGrowNAUOutput();
        } else if (trType.equals("AgMIP")) {
            LOG.info("AgMIP Weather Translator Started");
            newTranslator = new AgmipOutput();
        }
        if (translator != null) {
            LOG.debug("Translating with :"+translator.getClass().getName());
            Runnable thread;
            if (translator instanceof DividableOutputTranslator) {
                thread = new TranslateRunner(translator, data, path.toString(), trType, compress, divSize);
            } else {
                thread = new TranslateRunner(translator, data, path.toString(), trType, compress);
            }
            executor.execute(thread);
        } else if (newTranslator != null) {
            LOG.debug("Translating with :"+newTranslator.getClass().getName());
            Runnable thread = new TranslateRunner(newTranslator, aceData, path.toString(), trType, compress);
            executor.execute(thread);
        }
        
    }

    private static File createModelDestDirectory(String basePath, String model) {
        model = model.toUpperCase();
        File originalDestDir = new File(basePath+File.separator+model);
        File destDirectory = originalDestDir;
        int i=0;
        while (destDirectory.exists() && destDirectory.listFiles().length > 0) {
            i++;
            destDirectory = new File(originalDestDir.toString()+"-"+i);
        }
        destDirectory.mkdirs();
        return destDirectory;
    }
    
    private AceDataset toAceDataset(Map data) {
        
        AceDataset ace;
        try {
             ace = AceParser.parse(JSONAdapter.toJSON(data));
        } catch (IOException ex) {
            LOG.warn(ex.getMessage());
            ace = new AceDataset();
        }
        return ace;
    }
}
