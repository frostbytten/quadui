package org.agmip.ui.quadui;

import com.rits.cloning.Cloner;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.agmip.dome.DomeUtil;
import org.agmip.dome.Engine;
import org.agmip.translators.csv.AlnkInput;
import org.agmip.translators.csv.DomeInput;
import org.agmip.util.MapUtil;
import org.apache.pivot.util.concurrent.Task;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplyDomeTask extends Task<HashMap> {

    private static Logger log = LoggerFactory.getLogger(ApplyDomeTask.class);
    private final HashMap<String, HashMap<String, Object>> ovlDomes = new HashMap<String, HashMap<String, Object>>();
    private final HashMap<String, HashMap<String, Object>> stgDomes = new HashMap<String, HashMap<String, Object>>();
    private HashMap<String, Object> linkDomes = new HashMap<String, Object>();
    private HashMap<String, String> ovlLinks = new HashMap<String, String>();
    private HashMap<String, String> stgLinks = new HashMap<String, String>();
    private final HashMap<String, String> ovlNewDomeIdMap = new HashMap<String, String>();
    private final HashMap<String, String> stgNewDomeIdMap = new HashMap<String, String>();
//    private HashMap<String, ArrayList<String>> wthLinks = new HashMap<String, ArrayList<String>>();
//    private HashMap<String, ArrayList<String>> soilLinks = new HashMap<String, ArrayList<String>>();
    private HashMap source;
    private String mode;
    private boolean autoApply;
    private int thrPoolSize;

    public ApplyDomeTask(String linkFile, String fieldFile, String strategyFile, String mode, HashMap m, boolean autoApply) {
        this.source = m;
        this.mode = mode;
        this.autoApply = autoApply;
        // Setup the domes here.
        loadDomeLinkFile(linkFile);
        log.debug("link csv: {}", ovlLinks);

        if (mode.equals("strategy")) {
            loadDomeFile(strategyFile, stgDomes);
        }
        loadDomeFile(fieldFile, ovlDomes);
        thrPoolSize = Runtime.getRuntime().availableProcessors();
    }

    public ApplyDomeTask(String linkFile, String fieldFile, String strategyFile, String mode, HashMap m, boolean autoApply, int thrPoolSize) {
        this(linkFile, fieldFile, strategyFile, mode, m, autoApply);
        this.thrPoolSize = thrPoolSize;
    }

    private void loadDomeLinkFile(String fileName) {
        String fileNameTest = fileName.toUpperCase();
        log.debug("Loading LINK file: {}", fileName);
        linkDomes = null;

        try {
            if (fileNameTest.endsWith(".CSV")) {
                log.debug("Entering single ACMO CSV file DOME handling");
                AlnkInput reader = new AlnkInput();
                linkDomes = (HashMap<String, Object>) reader.readFile(fileName);
            } else if (fileNameTest.endsWith(".ALNK")) {
                log.debug("Entering single ALNK file DOME handling");
                AlnkInput reader = new AlnkInput();
                linkDomes = (HashMap<String, Object>) reader.readFile(fileName);
            }

            if (linkDomes != null) {
                log.debug("link info: {}", linkDomes.toString());
                try {
                    if (!linkDomes.isEmpty()) {
                        if (linkDomes.containsKey("link_overlay")) {
                            ovlLinks = (HashMap<String, String>) linkDomes.get("link_overlay");
                        }
                        if (linkDomes.containsKey("link_stragty")) {
                            stgLinks = (HashMap<String, String>) linkDomes.get("link_stragty");
                        }
                    }
                } catch (Exception ex) {
                    log.error("Error processing DOME file: {}", ex.getMessage());
                    HashMap<String, Object> d = new HashMap<String, Object>();
                    d.put("errors", ex.getMessage());
                }
            }
        } catch (Exception ex) {
            log.error("Error processing DOME file: {}", ex.getMessage());
            HashMap<String, Object> d = new HashMap<String, Object>();
            d.put("errors", ex.getMessage());
        }
    }

    private String getLinkIds(String domeType, HashMap entry) {
        String exname = MapUtil.getValueOr(entry, "exname", "");
        String wst_id = MapUtil.getValueOr(entry, "wst_id", "");
        String soil_id = MapUtil.getValueOr(entry, "soil_id", "");
        String linkIdsExp = getLinkIds(domeType, "EXNAME", exname);
        String linkIdsWst = getLinkIds(domeType, "WST_ID", wst_id);
        String linkIdsSoil = getLinkIds(domeType, "SOIL_ID", soil_id);
        String ret = "";
        if (!linkIdsExp.equals("")) {
            ret += linkIdsExp + "|";
        }
        if (!linkIdsWst.equals("")) {
            ret += linkIdsWst + "|";
        }
        if (!linkIdsSoil.equals("")) {
            ret += linkIdsSoil;
        }
        if (ret.endsWith("|")) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
    }

    private String getLinkIds(String domeType, String idType, String id) {
        HashMap<String, String> links;
        if (domeType.equals("strategy")) {
            links = stgLinks;
        } else if (domeType.equals("overlay")) {
            links = ovlLinks;
        } else {
            return "";
        }
        if (links.isEmpty() || id.equals("")) {
            return "";
        }
        String linkIds = "";
        ArrayList<String> altLinkIds = new ArrayList();
        altLinkIds.add(idType + "_ALL");
        if (id.matches("[^_]+_\\d+$")) {
            altLinkIds.add(idType + "_" + id.replaceAll("_\\d+$", ""));
            altLinkIds.add(idType + "_" + id + "__1");
        } else if (id.matches(".+_\\d+__\\d+$")) {
            altLinkIds.add(idType + "_" + id.replaceAll("__\\d+$", ""));
            altLinkIds.add(idType + "_" + id.replaceAll("_\\d+__\\d+$", ""));
        }
        altLinkIds.add(idType + "_" + id);
        for (String linkId : altLinkIds) {
            if (links.containsKey(linkId)) {
                linkIds += links.get(linkId) + "|";
            }
        }
        if (linkIds.endsWith("|")) {
            linkIds = linkIds.substring(0, linkIds.length() - 1);
        }
        return linkIds;
    }

    private void reviseDomeIds(HashMap entry, String domeIds, String domeType) {
        HashMap<String, HashMap<String, Object>> domes;
        HashMap<String, String> domeClimIdMap;
        String domeName;
        if (domeType.equals("strategy")) {
            domes = stgDomes;
            domeClimIdMap = ovlNewDomeIdMap;
            domeName = "seasonal_strategy";
        } else if (domeType.equals("overlay")) {
            domes = ovlDomes;
            domeClimIdMap = stgNewDomeIdMap;
            domeName = "field_overlay";
        } else {
            return;
        }
        
        StringBuilder newDomeIds = new StringBuilder();
        for (String domeId : domeIds.split("[|]")) {
            String[] metas = domeId.split("-");
            if (metas.length < 7) {
                if (domeClimIdMap.containsKey(domeId)) {
                    domeId = domeClimIdMap.get(domeId);
                } else {
                    String climId = "";
                    HashMap<String, Object> dome = MapUtil.getObjectOr(domes, domeId, new HashMap());
                    // Only auto-fix the clim_id for seasonal strategy DOME
                    if (!domeType.equals("overlay")) {
                        climId = MapUtil.getValueOr(entry, "clim_id", "").toUpperCase();
                        if (!dome.isEmpty()) {
                            ArrayList<HashMap<String, String>> rules = DomeUtil.getRules(dome);
                            for (HashMap<String, String> rule : rules) {
                                String var = MapUtil.getValueOr(rule, "variable", "").toLowerCase();
                                if (var.equals("clim_id")) {
                                    climId = MapUtil.getValueOr(rule, "args", climId).toUpperCase();
                                }
                            }
                        }
                    }
                    
                    StringBuilder newDomeId = new StringBuilder();
                    for (int i = 0; i < metas.length - 1; i++) {
                        newDomeId.append(metas[i]).append("-");
                    }
                    newDomeId.append(climId).append("-").append(metas[metas.length - 1]);
                    domeClimIdMap.put(domeId, newDomeId.toString());
                    domeId = newDomeId.toString();
                    DomeUtil.updateMetaInfo(dome, domeId);
                }
            }
            newDomeIds.append(domeId).append("|");
        }
        
        if (newDomeIds.charAt(newDomeIds.length() - 1) == '|') {
            newDomeIds.deleteCharAt(newDomeIds.length() - 1); 
        }
        entry.put(domeName, newDomeIds.toString());
    }

    private void loadDomeFile(String fileName, HashMap<String, HashMap<String, Object>> domes) {
        String fileNameTest = fileName.toUpperCase();

        log.info("Loading DOME file: {}", fileName);

        if (fileNameTest.endsWith(".ZIP")) {
            log.debug("Entering Zip file handling");
            ZipFile z;
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
            log.debug("Entering single CSV file DOME handling");
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
        } else if (fileNameTest.endsWith(".JSON") || fileNameTest.endsWith(".DOME")) {
            log.debug("Entering single ACE Binary file DOME handling");
            try {
                ObjectMapper mapper = new ObjectMapper();
                String json;
                if (fileNameTest.endsWith(".JSON")) {
                    json = new Scanner(new FileInputStream(fileName), "UTF-8").useDelimiter("\\A").next();
                } else {
                    json = new Scanner(new GZIPInputStream(new FileInputStream(fileName)), "UTF-8").useDelimiter("\\A").next();
                }
                HashMap<String, HashMap<String, Object>> tmp = mapper.readValue(json, new TypeReference<HashMap<String, HashMap<String, Object>>>() {
                });
//                domes.putAll(tmp);
                for (HashMap dome : tmp.values()) {
                    String domeName = DomeUtil.generateDomeName(dome);
                    if (!domeName.equals("----")) {
                        domes.put(domeName, new HashMap<String, Object>(dome));
                    }
                }
                log.debug("Domes layout: {}", domes.toString());
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

            if (!noExpMode) {
                updateWthReferences(updateExpReferences(true));
                flattenedData = MapUtil.flatPack(source);
            }
//            int cnt = 0;
//            for (HashMap<String, Object> entry : MapUtil.getRawPackageContents(source, "experiments")) {
//
//                log.debug("Exp at {}: {}, {}",
//                        cnt,
//                        entry.get("wst_id"),
//                        entry.get("clim_id"),
//                        ((HashMap) MapUtil.getObjectOr(entry, "weather", new HashMap())).get("wst_id"),
//                        ((HashMap) MapUtil.getObjectOr(entry, "weather", new HashMap())).get("clim_id")
//                        );
//                cnt++;
//            }
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
                String domeName = getLinkIds("strategy", entry);
                if (domeName.equals("")) {
                    domeName = MapUtil.getValueOr(entry, "seasonal_strategy", "");
                } else {
                    entry.put("seasonal_strategy", domeName);
                    log.debug("Apply seasonal strategy domes from link csv: {}", domeName);
                }
                entry.remove("seasonal_strategy");

                reviseDomeIds(entry, domeName, "strategy");
                String tmp[] = domeName.split("[|]");
                String strategyName;
                if (tmp.length > 1) {
                    log.warn("Multiple seasonal strategy dome is not supported yet, only the first dome will be applied");
                    for (int i = 1; i < tmp.length; i++) {
                        setFailedDomeId(entry, "seasonal_dome_failed", tmp[i]);
                    }
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
                        if (!noExpMode) {
                            // Check if there is no weather or soil data matched with experiment
                            if (((HashMap) MapUtil.getObjectOr(entry, "weather", new HashMap())).isEmpty()) {
                                log.warn("No scenario weather data found for: [{}]", MapUtil.getValueOr(entry, "exname", "N/A"));
                            }
                            if (((HashMap) MapUtil.getObjectOr(entry, "soil", new HashMap())).isEmpty()) {
                                log.warn("No soil data found for:    [{}]", MapUtil.getValueOr(entry, "exname", "N/A"));
                            }
                        }
                        ArrayList<HashMap<String, Object>> newEntries = generatorEngine.applyStg(flatSoilAndWthData(entry, noExpMode));
                        log.debug("New Entries to add: {}", newEntries.size());
                        strategyResults.addAll(newEntries);
                    } else {
                        log.error("Cannot find strategy: {}", strategyName);
                        setFailedDomeId(entry, "seasonal_dome_failed", strategyName);
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

        if (!noExpMode) {
            if (mode.equals("strategy")) {
                updateExpReferences(false);
            } else {
                updateWthReferences(updateExpReferences(false));
            }
            flattenedData = MapUtil.flatPack(source);
        }

        String ovlDomeName = "";
        if (autoApply) {
            for (String domeName : ovlDomes.keySet()) {
                ovlDomeName = domeName;
            }
            log.info("Auto apply field overlay: {}", ovlDomeName);
        }

        int cnt = 0;
        ArrayList<ApplyDomeRunner> engineRunners = new ArrayList();
        ExecutorService executor;
        if (thrPoolSize > 1) {
            log.info("Create the thread pool with the size of {} for appling filed overlay DOME", thrPoolSize);
            executor = Executors.newFixedThreadPool(thrPoolSize);
        } else if (thrPoolSize == 1) {
            log.info("Create the single thread pool for appling filed overlay DOME");
            executor = Executors.newSingleThreadExecutor();
        } else {
            log.info("Create the cached thread pool with flexible size for appling filed overlay DOME");
            executor = Executors.newCachedThreadPool();
        }
        HashMap<String, HashMap<String, ArrayList<HashMap<String, String>>>> soilDomeMap = new HashMap();
        HashMap<String, HashMap<String, ArrayList<HashMap<String, String>>>> wthDomeMap = new HashMap();
        HashSet<String> soilIds = getSWIdsSet("soils", new String[]{"soil_id"});
        HashSet<String> wthIds = getSWIdsSet("weathers", new String[]{"wst_id", "clim_id"});
        ArrayList<HashMap> soilDataArr = new ArrayList();
        ArrayList<HashMap> wthDataArr = new ArrayList();
        ArrayList<ArrayList<Engine>> soilEngines = new ArrayList();
        ArrayList<ArrayList<Engine>> wthEngines = new ArrayList();
        for (HashMap<String, Object> entry : flattenedData) {

            log.debug("Exp at {}: {}, {}, {}",
                    cnt,
                    entry.get("wst_id"),
                    ((HashMap) MapUtil.getObjectOr(entry, "weather", new HashMap())).get("wst_id"),
                    ((HashMap) MapUtil.getObjectOr(entry, "weather", new HashMap())).get("clim_id"));
            cnt++;
            if (autoApply) {
                entry.put("field_overlay", ovlDomeName);
            }
            String domeName = getLinkIds("overlay", entry);
            if (domeName.equals("")) {
                domeName = MapUtil.getValueOr(entry, "field_overlay", "");
            } else {
                entry.put("field_overlay", domeName);
                log.debug("Apply field overlay domes from link csv: {}", domeName);
            }

            reviseDomeIds(entry, domeName, "overlay");
            String soilId = MapUtil.getValueOr(entry, "soil_id", "");
            String wstId = MapUtil.getValueOr(entry, "wst_id", "");
            String climId = MapUtil.getValueOr(entry, "clim_id", "");
            ArrayList<Engine> sEngines = new ArrayList();
            ArrayList<Engine> wEngines = new ArrayList();
            String sDomeIds = "";
            String wDomeIds = "";
            ArrayList<HashMap<String, String>> sRulesTotal = new ArrayList();
            ArrayList<HashMap<String, String>> wRulesTotal = new ArrayList();
            if (!domeName.equals("")) {
                String tmp[] = domeName.split("[|]");
                int tmpLength = tmp.length;
                ArrayList<Engine> engines = new ArrayList();
                for (int i = 0; i < tmpLength; i++) {
                    String tmpDomeId = tmp[i].toUpperCase();
                    log.debug("Apply DOME {} for {}", tmpDomeId, MapUtil.getValueOr(entry, "exname", MapUtil.getValueOr(entry, "soil_id", MapUtil.getValueOr(entry, "wst_id", "<Unknow>"))));
                    log.debug("Looking for dome_name: {}", tmpDomeId);
                    if (ovlDomes.containsKey(tmpDomeId)) {
                        domeEngine = new Engine(ovlDomes.get(tmpDomeId));
                        entry.put("dome_applied", "Y");
                        entry.put("field_dome_applied", "Y");
                        ArrayList<HashMap<String, String>> sRules = domeEngine.extractSoilRules();
                        if (!sRules.isEmpty()) {
                            if (sDomeIds.equals("")) {
                                sDomeIds = tmpDomeId;
                            } else {
                                sDomeIds += "|" + tmpDomeId;
                            }
                            sEngines.add(new Engine(sRules, tmpDomeId));
                            sRulesTotal.addAll(sRules);
                        }
                        ArrayList<HashMap<String, String>> wRules = domeEngine.extractWthRules();
                        if (!wRules.isEmpty()) {
                            if (wDomeIds.equals("")) {
                                wDomeIds = tmpDomeId;
                            } else {
                                wDomeIds += "|" + tmpDomeId;
                            }
                            wEngines.add(new Engine(wRules, tmpDomeId));
                            wRulesTotal.addAll(wRules);
                        }
                        engines.add(domeEngine);
                    } else {
                        log.error("Cannot find overlay: {}", tmpDomeId);
                        setFailedDomeId(entry, "field_dome_failed", tmpDomeId);
                    }
                }
                HashMap<String, ArrayList<HashMap<String, String>>> lastAppliedSoilDomes = soilDomeMap.get(soilId);
                if (lastAppliedSoilDomes == null) {
                    soilDataArr.add(entry);
                    soilEngines.add(sEngines);
                    lastAppliedSoilDomes = new HashMap();
                    lastAppliedSoilDomes.put(sDomeIds, sRulesTotal);
                    soilDomeMap.put(soilId, lastAppliedSoilDomes);
                } else if (!lastAppliedSoilDomes.containsKey(sDomeIds)) {
                    boolean isSameRules = false;
                    for (ArrayList<HashMap<String, String>> rules : lastAppliedSoilDomes.values()) {
                        if (rules.equals(sRulesTotal)) {
                            isSameRules = true;
                            break;
                        }
                    }
                    if (!isSameRules) {
                        replicateSoil(entry, soilIds);
                        soilDataArr.add(entry);
                        soilEngines.add(sEngines);
                        lastAppliedSoilDomes.put(sDomeIds, sRulesTotal);
                    }
                }
                HashMap<String, ArrayList<HashMap<String, String>>> lastAppliedWthDomes = wthDomeMap.get(wstId+climId);
                if (lastAppliedWthDomes == null) {
                    wthDataArr.add(entry);
                    wthEngines.add(wEngines);
                    lastAppliedWthDomes = new HashMap();
                    lastAppliedWthDomes.put(wDomeIds, wRulesTotal);
                    wthDomeMap.put(wstId+climId, lastAppliedWthDomes);
                } else if (!lastAppliedWthDomes.containsKey(wDomeIds)) {
                    boolean isSameRules = false;
                    for (ArrayList<HashMap<String, String>> rules : lastAppliedWthDomes.values()) {
                        if (rules.equals(wRulesTotal)) {
                            isSameRules = true;
                            break;
                        }
                    }
                    if (!isSameRules) {
                        replicateWth(entry, wthIds);
                        wthDataArr.add(entry);
                        wthEngines.add(wEngines);
                        lastAppliedWthDomes.put(wDomeIds, wRulesTotal);
                    }
                }
                engineRunners.add(new ApplyDomeRunner(engines, entry, noExpMode, mode));
            }
        }

        for (int i = 0; i < soilDataArr.size(); i++) {
            for (Engine e : soilEngines.get(i)) {
                e.apply(flatSoilAndWthData(soilDataArr.get(i), noExpMode));
            }
        }
        for (int i = 0; i < wthDataArr.size(); i++) {
            for (Engine e : wthEngines.get(i)) {
                e.apply(flatSoilAndWthData(wthDataArr.get(i), noExpMode));
            }
        }
        for (ApplyDomeRunner engineRunner : engineRunners) {
            executor.submit(engineRunner);
//            engine.apply(flatSoilAndWthData(entry, noExpMode));
//            ArrayList<String> strategyList = engine.getGenerators();
//            if (!strategyList.isEmpty()) {
//                log.warn("The following DOME commands in the field overlay file are ignored : {}", strategyList.toString());
//            }
//            if (!noExpMode && !mode.equals("strategy")) {
//                // Check if there is no weather or soil data matched with experiment
//                if (((HashMap) MapUtil.getObjectOr(entry, "weather", new HashMap())).isEmpty()) {
//                    log.warn("No baseline weather data found for: [{}]", MapUtil.getValueOr(entry, "exname", "N/A"));
//                }
//                if (((HashMap) MapUtil.getObjectOr(entry, "soil", new HashMap())).isEmpty()) {
//                    log.warn("No soil data found for:    [{}]", MapUtil.getValueOr(entry, "exname", "N/A"));
//                }
//            }
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
        }
//        executor = null;

        if (noExpMode) {
            output.put("domeoutput", source);
        } else {
            output.put("domeoutput", MapUtil.bundle(flattenedData));
        }
        if (ovlDomes != null && !ovlDomes.isEmpty()) {
            output.put("ovlDomes", ovlDomes);
        }
        if (stgDomes != null && !stgDomes.isEmpty()) {
            output.put("stgDomes", stgDomes);
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

    private void setFailedDomeId(HashMap data, String failKey, String failId) {
        String failIds;
        if ((failIds = (String) data.get(failKey)) != null) {
            data.put(failKey, failId);
        } else {
            data.put(failKey, failIds + "|" + failId);
        }
    }

    private boolean updateExpReferences(boolean isStgDome) {

        ArrayList<HashMap<String, Object>> expArr = MapUtil.getRawPackageContents(source, "experiments");
        boolean isClimIDchanged = false;

        HashMap<String, HashMap<String, Object>> domes;
        String linkid;
        String domeKey;
        int maxDomeNum;
        if (isStgDome) {
            domes = stgDomes;
            linkid = "strategy";
            domeKey = "seasonal_strategy";
            maxDomeNum = 1;
        } else {
            domes = ovlDomes;
            linkid = "field";
            domeKey = "field_overlay";
            maxDomeNum = Integer.MAX_VALUE;
        }

        // Pre-scan the seasnal DOME to update reference variables
        String autoDomeName = "";
        if (autoApply) {
            for (String domeName : domes.keySet()) {
                autoDomeName = domeName;
            }
        }
        for (HashMap<String, Object> exp : expArr) {

            String domeName = getLinkIds(linkid, exp);
            if (domeName.equals("")) {
                if (autoApply) {
                    domeName = autoDomeName;
                } else {
                    domeName = MapUtil.getValueOr(exp, domeKey, "");
                }
            }

            if (!domeName.equals("")) {
                String tmp[] = domeName.split("[|]");
                int tmpLength = Math.min(tmp.length, maxDomeNum);
                for (int i = 0; i < tmpLength; i++) {
                    String tmpDomeId = tmp[i].toUpperCase();
                    log.debug("Looking for dome_name: {}", tmpDomeId);
                    if (domes.containsKey(tmpDomeId)) {
                        log.debug("Found DOME {}", tmpDomeId);
                        Engine domeEngine = new Engine(domes.get(tmpDomeId));
                        isClimIDchanged = domeEngine.updateWSRef(exp, isStgDome, mode.equals("strategy"));
                        // Check if the wst_id is switch to 8-bit long version
                        String wst_id = MapUtil.getValueOr(exp, "wst_id", "");
                        if (isStgDome && wst_id.length() < 8) {
                            exp.put("wst_id", wst_id + "0XXX");
                            exp.put("clim_id", "0XXX");
                            isClimIDchanged = true;
                        }
                        log.debug("New exp linkage: {}", exp);
                    }
                }
            }
        }
        return isClimIDchanged;
    }

    private void updateWthReferences(boolean isClimIDchanged) {

        ArrayList<HashMap<String, Object>> wthArr = MapUtil.getRawPackageContents(source, "weathers");
        boolean isStrategy = mode.equals("strategy");
        HashMap<String, HashMap> unfixedWths = new HashMap();
        HashSet<String> fixedWths = new HashSet();
        for (HashMap<String, Object> wth : wthArr) {
            String wst_id = MapUtil.getValueOr(wth, "wst_id", "");
            String clim_id = MapUtil.getValueOr(wth, "clim_id", "");
            if (clim_id.equals("")) {
                if (wst_id.length() == 8) {
                    clim_id = wst_id.substring(4, 8);
                } else {
                    clim_id = "0XXX";
                }
            }
            // If user assign CLIM_ID in the DOME, or find non-baseline data in the overlay mode, then switch WST_ID to 8-bit version
            if (isStrategy || isClimIDchanged || !clim_id.startsWith("0")) {
                if (wst_id.length() < 8) {
                    wth.put("wst_id", wst_id + clim_id);
                }
            } else {
                // Temporally switch all the WST_ID to 8-bit in the data set
                if (wst_id.length() < 8) {
                    wth.put("wst_id", wst_id + clim_id);
                } else {
                    wst_id = wst_id.substring(0, 4);
                }
                // Check if there is multiple baseline record for one site
                if (unfixedWths.containsKey(wst_id)) {
                    log.warn("There is multiple baseline weather data for site [{}], please choose a particular baseline via field overlay DOME", wst_id);
                    unfixedWths.remove(wst_id);
                    fixedWths.add(wst_id);

                } else {
                    if (!fixedWths.contains(wst_id)) {
                        unfixedWths.put(wst_id, wth);
                    }
                }
            }
        }

        // If no CLIM_ID provided in the overlay mode, then switch the baseline WST_ID to 4-bit.
        if (!isStrategy && !unfixedWths.isEmpty()) {
            for (String wst_id : unfixedWths.keySet()) {
                unfixedWths.get(wst_id).put("wst_id", wst_id);
            }
        }
    }

    private void replicateSoil(HashMap entry, HashSet soilIds) {
        String newSoilId = MapUtil.getValueOr(entry, "soil_id", "");
        HashMap data = MapUtil.getObjectOr(entry, "soil", new HashMap());
        if (data.isEmpty()) {
            return;
        }
        Cloner cloner = new Cloner();
        HashMap newData = cloner.deepClone(data);
        ArrayList<HashMap<String, Object>> soils = MapUtil.getRawPackageContents(source, "soils");
        int count = 1;
        while (soilIds.contains(newSoilId + "_" + count)) {
            count++;
        }
        newSoilId += "_" + count;
        newData.put("soil_id", newSoilId);
        entry.put("soil_id", newSoilId);
        entry.put("soil", newData);
        soilIds.add(newSoilId);
        soils.add(newData);
    }

    private void replicateWth(HashMap entry, HashSet wthIds) {
        String newWthId = MapUtil.getValueOr(entry, "wst_id", "");
        String climId = MapUtil.getValueOr(entry, "clim_id", "");
        HashMap data = MapUtil.getObjectOr(entry, "weather", new HashMap());
        if (data.isEmpty()) {
            return;
        }
        Cloner cloner = new Cloner();
        HashMap newData = cloner.deepClone(data);
        ArrayList<HashMap<String, Object>> wths = MapUtil.getRawPackageContents(source, "weathers");
        String inst;
        if (newWthId.length() > 1) {
            inst = newWthId.substring(0, 2);
        } else {
            inst = newWthId + "0";
        }
        newWthId = inst + "01" + climId;
        int count = 1;
        while (wthIds.contains(newWthId) && count < 99) {
            count++;
            newWthId = String.format("%s%02d%s", inst, count, climId);
        }
        if (count == 99 && wthIds.contains(newWthId)) {
            inst = inst.substring(0, 1);
            newWthId = inst + "100" + climId;
            while (wthIds.contains(newWthId)) {
                count++;
                newWthId = String.format("%s%03d%s", inst, count, climId);
            }
        }
        newData.put("wst_id", newWthId);
        entry.put("wst_id", newWthId);
        entry.put("weather", newData);
        wthIds.add(newWthId);
        wths.add(newData);
    }

    private HashSet<String> getSWIdsSet(String dataKey, String... idKeys) {
        HashSet<String> ret = new HashSet();
        ArrayList<HashMap<String, Object>> arr = MapUtil.getRawPackageContents(source, dataKey);
        for (HashMap data : arr) {
            StringBuilder sb = new StringBuilder();
            for (String idKey : idKeys) {
                sb.append(MapUtil.getValueOr(data, idKey, ""));
            }
            ret.add(sb.toString());
        }
        return ret;
    }
}
