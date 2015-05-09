package org.agmip.ui.quadui;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceSoil;
import org.agmip.ace.AceWeather;
import org.agmip.ace.io.AceParser;
import org.agmip.common.Functions;

import org.apache.pivot.util.concurrent.Task;
import org.agmip.core.types.TranslatorInput;
import org.agmip.translators.csv.CSVInput;
import org.agmip.translators.dssat.DssatControllerInput;
import org.agmip.translators.agmip.AgmipInput;
import org.agmip.translators.apsim.ApsimReader;
import org.agmip.util.JSONAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TranslateFromTask extends Task<HashMap> {

    private static final Logger LOG = LoggerFactory.getLogger(TranslateFromTask.class);
    private final HashMap<String, HashMap<String, TranslatorInput>> fileTranslatorsMap = new HashMap();

    public TranslateFromTask(String... files) throws Exception {
        for (String file : files) {
            HashMap<String, TranslatorInput> translators = new HashMap<String, TranslatorInput>();
            fileTranslatorsMap.put(file, translators);
            if (file.toLowerCase().endsWith(".zip")) {
                FileInputStream f = new FileInputStream(file);
                ZipInputStream z = new ZipInputStream(new BufferedInputStream(f));
                ZipEntry ze;
                while ((ze = z.getNextEntry()) != null) {
                    String zeName = ze.getName().toLowerCase();
                    if (zeName.endsWith(".csv")) {
                        translators.put("CSV", new CSVInput());
    //                    break;
                    } else if (zeName.endsWith(".wth") || 
                            zeName.endsWith(".sol") ||
                            zeName.matches(".+\\.\\d{2}[xat]")) {
                        translators.put("DSSAT", new DssatControllerInput());
    //                    break;
                    } else if (zeName.endsWith(".agmip")) {
                        LOG.debug("Found .AgMIP file {}", ze.getName());
                        translators.put("AgMIP", new AgmipInput());
    //                    break;
                    } else if (zeName.endsWith(".met")) {
                        LOG.debug("Found .met file {}", ze.getName());
                        translators.put("APSIM", new ApsimReader());
    //                    break;
                    } else if (zeName.endsWith(".aceb")) {
                        LOG.debug("Found .ACEB file {}", ze.getName());
                        translators.put("ACEB", new AcebInput());
                    } else if (zeName.endsWith(".json")) {
                        LOG.debug("Found .JSON file {}", ze.getName());
                        translators.put("JSON", new JsonInput());
                    } else if (ze.isDirectory() && zeName.endsWith("_specific/")) {
                        LOG.debug("Found model specific folder {}", ze.getName());
                        translators.put("ModelSpec", new ModelFileDumperInput());
                    }
                }
                if (translators.isEmpty()) {
                    translators.put("DSSAT", new DssatControllerInput());
                }
                z.close();
                f.close();
            } else if (file.toLowerCase().endsWith(".agmip")) {
                translators.put("AgMIP", new AgmipInput());
            } else if (file.toLowerCase().endsWith(".met")) {
                translators.put("APSIM", new ApsimReader());
            } else if (file.toLowerCase().endsWith(".csv")) {
                translators.put("CSV", new CSVInput());
            } else if (file.toLowerCase().endsWith(".aceb")) {
                translators.put("ACEB", new AcebInput());
            } else if (file.toLowerCase().endsWith(".json")) {
                translators.put("JSON", new JsonInput());
            } else if (file.toLowerCase().endsWith(".sol") ||
                    file.toLowerCase().endsWith(".wth") ||
                    file.toLowerCase().matches(".+\\.\\d{2}[xat]")) {
                translators.put("DSSAT", new DssatControllerInput());
            } else {
                LOG.error("Unsupported file: {}", file);
                throw new Exception("Unsupported file type");
            }
        }
    }

    @Override
    public HashMap<String, Object> execute() {
        HashMap<String, Object> output = new HashMap<String, Object>();
        try {
            for (String file : fileTranslatorsMap.keySet()) {
                HashMap<String, TranslatorInput> translators = fileTranslatorsMap.get(file);
                for (String key : translators.keySet()) {
                    LOG.info("{} translator is fired to read {}", key, file);
                    Map m = translators.get(key).readFile(file);
                    if (key.equals("ModelSpec")) {
                        output.put("ModelSpec", m);
                    } else {
                        combineResult(output, m);
                    }
                    LOG.debug("{}", output.get("weathers"));
                }
            }
            
            // Only use in extreme cases
            //LOG.debug("Translate From Results: {}", output.toString());
            return output;
        } catch (Exception ex) {
            LOG.error(Functions.getStackTrace(ex));
            output.put("errors", ex.toString());
            return output;
        }
    }

    private void combineResult(HashMap out, Map in) {
        String[] keys = {"experiments", "soils", "weathers"};
        for (String key : keys) {
            ArrayList outArr;
            ArrayList inArr;
            if ((inArr = (ArrayList) in.get(key)) != null && !inArr.isEmpty()) {
                outArr = (ArrayList) out.get(key);
                if (outArr == null) {
                    out.put(key, inArr);
                } else {
                    outArr.addAll(inArr);
                }
            }
        }
    }

    private class JsonInput implements TranslatorInput {

        @Override
        public Map readFile(String file) throws Exception {
            HashMap ret = new HashMap();
            if (file.toLowerCase().endsWith(".zip")) {
                ZipFile zf = new ZipFile(file);
                Enumeration<? extends ZipEntry> e = zf.entries();
                while (e.hasMoreElements()) {
                    ZipEntry ze = (ZipEntry) e.nextElement();
                    LOG.debug("Entering file: " + ze);
                    if (ze.getName().toLowerCase().endsWith(".json")) {
                        combineResult(ret, readJson(zf.getInputStream(ze)));
                    }
                }
                zf.close();
            } else {
                ret = readJson(new FileInputStream(file));
            }
            return ret;
        }

    }

    private class AcebInput implements TranslatorInput {

        @Override
        public Map readFile(String file) throws Exception {
            HashMap ret = new HashMap();
            if (file.toLowerCase().endsWith(".zip")) {
                ZipFile zf = new ZipFile(file);
                Enumeration<? extends ZipEntry> e = zf.entries();
                while (e.hasMoreElements()) {
                    ZipEntry ze = (ZipEntry) e.nextElement();
                    LOG.debug("Entering file: " + ze);
                    if (ze.getName().toLowerCase().endsWith(".aceb")) {
                        combineResult(ret, readAceb(zf.getInputStream(ze)));
                    }
                }
                zf.close();
            } else {
                ret = readAceb(new FileInputStream(file));
            }
            return ret;
        }

    }

    private HashMap readAceb(InputStream fileStream) throws Exception {
        HashMap data = new HashMap();
        try {
            AceDataset ace = AceParser.parseACEB(fileStream);
            ace.linkDataset();
            ArrayList<HashMap> arr;
            // Experiments
            arr = new ArrayList();
            for (AceExperiment exp : ace.getExperiments()) {
                arr.add(JSONAdapter.fromJSON(new String(exp.rebuildComponent())));
            }
            if (!arr.isEmpty()) {
                data.put("experiments", arr);
            }
            // Soils
            arr = new ArrayList();
            for (AceSoil soil : ace.getSoils()) {
                arr.add(JSONAdapter.fromJSON(new String(soil.rebuildComponent())));
            }
            if (!arr.isEmpty()) {
                data.put("soils", arr);
            }
            // Weathers
            arr = new ArrayList();
            for (AceWeather wth : ace.getWeathers()) {
                arr.add(JSONAdapter.fromJSON(new String(wth.rebuildComponent())));
            }
            if (!arr.isEmpty()) {
                data.put("weathers", arr);
            }
        } catch (Exception ex) {
            LOG.error(Functions.getStackTrace(ex));
        }
        return data;
    }

    private HashMap readJson(InputStream fileStream) throws Exception {
        
        StringBuilder jsonStr = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(fileStream));
        String line;
        while ((line = br.readLine()) != null) {
            jsonStr.append(line.trim());
        }
        br.close();
        return JSONAdapter.fromJSON(jsonStr.toString());
    }
}
