package org.agmip.ui.quadui;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.agmip.dome.DomeUtil;
import org.agmip.dome.Engine;
import org.agmip.translators.csv.DomeInput;
import org.agmip.util.MapUtil;
import org.apache.pivot.util.concurrent.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplyDomeTask extends Task<HashMap> {

    private static Logger log = LoggerFactory.getLogger(ApplyDomeTask.class);
    private HashMap<String, HashMap<String, Object>> ovlDomes = new HashMap<String, HashMap<String, Object>>();
    private HashMap<String, HashMap<String, Object>> stgDomes = new HashMap<String, HashMap<String, Object>>();
//    private HashMap<String, ArrayList<String>> links = new HashMap<String, ArrayList<String>>();
//    private HashMap<String, ArrayList<String>> wthLinks = new HashMap<String, ArrayList<String>>();
//    private HashMap<String, ArrayList<String>> soilLinks = new HashMap<String, ArrayList<String>>();
    private HashMap source;
    private String mode;
    private boolean autoApply;


//    public ApplyDomeTask(String linkFile, String fieldFile, String strategyFile, String mode, HashMap m) {
    public ApplyDomeTask(String fieldFile, String strategyFile, String mode, HashMap m, boolean autoApply) {
        this.source = m;
        this.mode = mode;
        this.autoApply = autoApply;
        // Setup the domes here.

        if (mode.equals("strategy")) {
            loadDomeFile(strategyFile, stgDomes);
        }
        loadDomeFile(fieldFile, ovlDomes);
    }

//    private void loadDomeLinkFile(String fileName) {
//        String fileNameTest = fileName.toUpperCase();
//
//        log.debug("Loading LINK file: {}", fileName);
//
//        if (fileNameTest.endsWith(".ZIP")) {
//            log.debug("Entering Zip file handling");
//            ZipFile z = null;
//            try {
//                z = new ZipFile(fileName);
//                Enumeration  entries = z.entries();
//                while (entries.hasMoreElements()) {
//                    // Do we handle nested zips? Not yet.
//                    ZipEntry entry = (ZipEntry) entries.nextElement();
//                    File zipFileName = new File(entry.getName());
//                    if (zipFileName.getName().toLowerCase().endsWith(".csv") && ! zipFileName.getName().startsWith(".")) {
//                        log.debug("Processing file: {}", zipFileName.getName());
//                        DomeInput translator = new DomeInput();
//                        translator.readCSV(z.getInputStream(entry));
//                        HashMap<String, Object> dome = translator.getDome();
//                        log.debug("dome info: {}", dome.toString());
//                        String domeName = DomeUtil.generateDomeName(dome);
//                        if (! domeName.equals("----")) {
////                            links.put(domeName, new HashMap<String, Object>(dome));
//                        }
//                    }
//                }
//                z.close();
//            } catch (Exception ex) {
//                log.error("Error processing DOME file: {}", ex.getMessage());
//                HashMap<String, Object> d = new HashMap<String, Object>();
//                d.put("errors", ex.getMessage());
//            }
//        } else if (fileNameTest.endsWith(".CSV")) {
//            log.debug("Entering single file DOME handling");
//            try {
//                DomeInput translator = new DomeInput();
//                HashMap<String, Object> dome = (HashMap<String, Object>) translator.readFile(fileName);
//                String domeName = DomeUtil.generateDomeName(dome);
//                log.debug("Dome name: {}", domeName);
//                log.debug("Dome layout: {}", dome.toString());
//
////                links.put(domeName, dome);
//            } catch (Exception ex) {
//                log.error("Error processing DOME file: {}", ex.getMessage());
//                HashMap<String, Object> d = new HashMap<String, Object>();
//                d.put("errors", ex.getMessage());
//            }
//        }
//    }
    private void loadDomeFile(String fileName, HashMap<String, HashMap<String, Object>> domes) {
        String fileNameTest = fileName.toUpperCase();

        log.info("Loading DOME file: {}", fileName);

        if (fileNameTest.endsWith(".ZIP")) {
            log.debug("Entering Zip file handling");
            ZipFile z = null;
            try {
                z = new ZipFile(fileName);
                Enumeration entries = z.entries();
                while (entries.hasMoreElements()) {
                    // Do we handle nested zips? Not yet.
                    ZipEntry entry = (ZipEntry) entries.nextElement();
                    File zipFileName = new File(entry.getName());
                    if (zipFileName.getName().toLowerCase().endsWith(".csv") && !zipFileName.getName().startsWith(".")) {
                        log.debug("Processing file: {}", zipFileName.getName());
                        DomeInput translator = new DomeInput();
                        translator.readCSV(z.getInputStream(entry));
                        HashMap<String, Object> dome = translator.getDome();
                        log.debug("dome info: {}", dome.toString());
                        String domeName = DomeUtil.generateDomeName(dome);
                        if (!domeName.equals("----")) {
                            domes.put(domeName, new HashMap<String, Object>(dome));
                        }
                    }
                }
                z.close();
            } catch (Exception ex) {
                log.error("Error processing DOME file: {}", ex.getMessage());
                HashMap<String, Object> d = new HashMap<String, Object>();
                d.put("errors", ex.getMessage());
            }
        } else if (fileNameTest.endsWith(".CSV")) {
            log.debug("Entering single file DOME handling");
            try {
                DomeInput translator = new DomeInput();
                HashMap<String, Object> dome = (HashMap<String, Object>) translator.readFile(fileName);
                String domeName = DomeUtil.generateDomeName(dome);
                log.debug("Dome name: {}", domeName);
                log.debug("Dome layout: {}", dome.toString());

                domes.put(domeName, dome);
            } catch (Exception ex) {
                log.error("Error processing DOME file: {}", ex.getMessage());
                HashMap<String, Object> d = new HashMap<String, Object>();
                d.put("errors", ex.getMessage());
            }
        }
    }

    @Override
    public HashMap<String, Object> execute() {
        // First extract all the domes and put them in a HashMap by DOME_NAME
        // The read the DOME_NAME field of the CSV file
        // Split the DOME_NAME, and then apply sequentially to the HashMap.

        // PLEASE NOTE: This can be a massive undertaking if the source map
        // is really large. Need to find optimization points.

        HashMap<String, Object> output = new HashMap<String, Object>();
        //HashMap<String, ArrayList<HashMap<String, String>>> dome;
        // Load the dome

        if (ovlDomes.isEmpty() && stgDomes.isEmpty()) {
            log.info("No DOME to apply.");
            HashMap<String, Object> d = new HashMap<String, Object>();
            //d.put("domeinfo", new HashMap<String, String>());
            d.put("domeoutput", source);
            return d;
        }

        if (autoApply) {
            HashMap<String, Object> d = new HashMap<String, Object>();
            if (ovlDomes.size() > 1) {
                log.error("Auto-Apply feature only allows one field overlay file per run");
                d.put("errors", "Auto-Apply feature only allows one field overlay file per run");
                return d;
            } else if (stgDomes.size() > 1) {
                log.error("Auto-Apply feature only allows one seasonal strategy file per run");
                d.put("errors", "Auto-Apply feature only allows one seasonal strategy file per run");
                return d;
            }
        }

        // Flatten the data and apply the dome.
        Engine domeEngine;
        ArrayList<HashMap<String, Object>> flattenedData = MapUtil.flatPack(source);
        boolean noExpMode = false;
        if (flattenedData.isEmpty()) {
            log.info("No experiment data detected, will try Weather and Soil data only mode");
            noExpMode = true;
            flattenedData.addAll(MapUtil.getRawPackageContents(source, "soils"));
            flattenedData.addAll(MapUtil.getRawPackageContents(source, "weathers"));
//            flatSoilAndWthData(flattenedData, "soil");
//            flatSoilAndWthData(flattenedData, "weather");
            if (flattenedData.isEmpty()) {
                HashMap<String, Object> d = new HashMap<String, Object>();
                log.error("No data found from input file, no DOME will be applied for data set {}", source.toString());
                d.put("errors", "Loaded raw data is invalid, please check input files");
                return d;
            }
        }

        if (mode.equals("strategy")) {
            log.debug("Domes: {}", stgDomes.toString());
            log.debug("Entering Strategy mode!");

            String stgDomeName = "";
            if (autoApply) {
                for (String domeName : stgDomes.keySet()) {
                    stgDomeName = domeName;
                }
                log.info("Auto apply seasonal strategy: {}", stgDomeName);
            }
            Engine generatorEngine;
            ArrayList<HashMap<String, Object>> strategyResults = new ArrayList<HashMap<String, Object>>();
            for (HashMap<String, Object> entry : flattenedData) {
                if (autoApply) {
                    entry.put("seasonal_strategy", stgDomeName);
                }
                String domeName = MapUtil.getValueOr(entry, "seasonal_strategy", "");
                String tmp[] = domeName.split("[|]");
                String strategyName;
                if (tmp.length > 1) {
                    log.warn("Multiple seasonal strategy dome is not supported yet, only the first dome will be applied");
                }
                strategyName = tmp[0];

                log.info("Apply DOME {} for {}", strategyName, MapUtil.getValueOr(entry, "exname", MapUtil.getValueOr(entry, "soil_id", MapUtil.getValueOr(entry, "wst_id", "<Unknow>"))));
                log.debug("Looking for ss: {}", strategyName);
                if (!strategyName.equals("")) {
                    if (stgDomes.containsKey(strategyName)) {
                        log.debug("Found strategyName");
                        entry.put("dome_applied", "Y");
                        entry.put("seasonal_dome_applied", "Y");
                        generatorEngine = new Engine(stgDomes.get(strategyName), true);
                        ArrayList<HashMap<String, Object>> newEntries = generatorEngine.applyStg(flatSoilAndWthData(entry, noExpMode));
                        log.debug("New Entries to add: {}", newEntries.size());
                        strategyResults.addAll(newEntries);
                    } else {
                        log.error("Cannot find strategy: {}", strategyName);
                    }
                }
            }
            log.debug("=== FINISHED GENERATION ===");
            log.debug("Generated count: {}", strategyResults.size());
            ArrayList<HashMap<String, Object>> exp = MapUtil.getRawPackageContents(source, "experiments");
            exp.clear();
            exp.addAll(strategyResults);
            flattenedData = MapUtil.flatPack(source);
            if (noExpMode) {
                flattenedData.addAll(MapUtil.getRawPackageContents(source, "soils"));
                flattenedData.addAll(MapUtil.getRawPackageContents(source, "weathers"));
            }
        }

        String ovlDomeName = "";
        if (autoApply) {
            for (String domeName : ovlDomes.keySet()) {
                ovlDomeName = domeName;
            }
            log.info("Auto apply field overlay: {}", ovlDomeName);
        }

        for (HashMap<String, Object> entry : flattenedData) {

            if (autoApply) {
                entry.put("field_overlay", ovlDomeName);
            }
            String domeName = MapUtil.getValueOr(entry, "field_overlay", "");
            if (!domeName.equals("")) {
                String tmp[] = domeName.split("[|]");
                int tmpLength = tmp.length;
                for (int i = 0; i < tmpLength; i++) {
                    String tmpDomeId = tmp[i].toUpperCase();
                    log.info("Apply DOME {} for {}", tmpDomeId, MapUtil.getValueOr(entry, "exname", MapUtil.getValueOr(entry, "soil_id", MapUtil.getValueOr(entry, "wst_id", "<Unknow>"))));
                    log.debug("Looking for dome_name: {}", tmpDomeId);
                    if (ovlDomes.containsKey(tmpDomeId)) {
                        domeEngine = new Engine(ovlDomes.get(tmpDomeId));
                        entry.put("dome_applied", "Y");
                        entry.put("field_dome_applied", "Y");
                        domeEngine.apply(flatSoilAndWthData(entry, noExpMode));
                        ArrayList<String> strategyList = domeEngine.getGenerators();
                        if (!strategyList.isEmpty()) {
                            log.warn("The following DOME commands in the field overlay file are ignored : {}", strategyList.toString());
                        }
                    } else {
                        log.error("Cannot find overlay: {}", tmpDomeId);
                    }
                }
            }
        }

        if (noExpMode) {
            output.put("domeoutput", source);
        } else {
            output.put("domeoutput", MapUtil.bundle(flattenedData));
        }
        return output;
    }

//    private void flatSoilAndWthData(ArrayList<HashMap<String, Object>> flattenedData, String key) {
//        ArrayList<HashMap<String, Object>> arr = MapUtil.getRawPackageContents(source, key + "s");
//        for (HashMap<String, Object> data : arr) {
//            HashMap<String, Object> tmp = new HashMap<String, Object>();
//            tmp.put(key, data);
//            flattenedData.add(tmp);
//        }
//    }

    private HashMap<String, Object> flatSoilAndWthData(HashMap<String, Object> data, boolean noExpFlg) {

        if (!noExpFlg) {
            return data;
        }

        HashMap<String, Object> ret;
        if (data.containsKey("dailyWeather")) {
            ret = new HashMap<String, Object>();
            ret.put("weather", data);
        } else if (data.containsKey("soilLayer")) {
            ret = new HashMap<String, Object>();
            ret.put("soil", data);
        } else {
            ret = data;
        }
        return ret;

    }
}
