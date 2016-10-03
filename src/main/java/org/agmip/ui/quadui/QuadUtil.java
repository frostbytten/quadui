package org.agmip.ui.quadui;

import com.google.common.io.Files;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceSoil;
import org.agmip.ace.AceWeather;
import org.agmip.ace.io.AceParser;
import org.agmip.common.Functions;
import org.agmip.dome.BatchEngine;
import org.agmip.translators.csv.BatchInput;
import org.agmip.util.JSONAdapter;
import static org.agmip.util.JSONAdapter.toJSON;
import org.agmip.util.MapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Meng Zhang
 */
public class QuadUtil {
    
    private static final Logger LOG = LoggerFactory.getLogger(QuadUtil.class);
    
    public static HashMap<String, Object> loadBatchFile(String fileName) {
        String fileNameTest = fileName.toUpperCase();
        LOG.debug("Loading Batch file: {}", fileName);
        HashMap batDome = null;

        try {
            if (fileNameTest.endsWith(".CSV")) {
                LOG.debug("Entering Batch CSV file handling");
                BatchInput reader = new BatchInput();
                batDome = (HashMap<String, Object>) reader.readFile(fileName);
            } else if (fileNameTest.endsWith(".DOME")) {
                // TODO
            }

            return batDome;
        } catch (Exception ex) {
            LOG.error("Error processing DOME file: {}", ex.getMessage());
            return new HashMap();
        }
    }
    
    public static String getOutputDir(String path, boolean isOverwrite) {
        return getOutputDir(path, isOverwrite, null);
    }

    public static String getOutputDir(String path, boolean isOverwrite, BatchEngine batEngine) {
        if (batEngine != null) {
            path += File.separator + "batch-" + batEngine.getNextGroupId();
            File dir = new File(path);
            int count = 0;
            while (dir.exists() && dir.listFiles().length > 0) {
                if (cleanBatchDir(path, isOverwrite)) {
                    break;
                }
                count++;
                dir = new File(path + "_" + count);
            }
            if (count > 0) {
                path += "_" + count;
            }
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }

        return path;
    }

    public static boolean cleanBatchDir(String path, boolean isOverwrite) {
        if (isOverwrite) {
            if (Functions.clearDirectory(new File(path))) {
                return true;
            } else {
                File dir = new File(path);
                for (File f : dir.listFiles()) {
                    if (f.isDirectory() && f.listFiles().length != 0) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }

    }
    
    public static String getCurGroupId(BatchEngine batEngine, boolean isBatchApplied) {
        if (batEngine == null) {
            return "";
        } else {
            if (isBatchApplied) {
                return batEngine.getCurGroupId();
            } else {
              return batEngine.getNextGroupId();
            }

        }
    }

    public static String getCurBatchInfo(BatchEngine batEngine, boolean isBatchApplied) {
        String curGroupId = getCurGroupId(batEngine, isBatchApplied);
        if (curGroupId.equals("")) {
            return "";
        } else {
            return "[Batch-" + curGroupId + "] ";
        }
    }

    public static boolean isDomeApplied(String filePath, HashMap data) {
        boolean isDomeApplied = false;
        if (filePath.endsWith(".json")) {
            // Check if the data has been applied with DOME.
            ArrayList<HashMap> exps = MapUtil.getObjectOr(data, "experiments", new ArrayList());
            for (HashMap exp : exps) {
                if (MapUtil.getValueOr(exp, "dome_applied", "").equals("Y")) {
                    isDomeApplied = true;
                    break;
                }
            }
            if (exps.isEmpty()) {
                ArrayList<HashMap> soils = MapUtil.getObjectOr(data, "soils", new ArrayList());
                ArrayList<HashMap> weathers = MapUtil.getObjectOr(data, "weathers", new ArrayList());
                for (HashMap soil : soils) {
                    if (MapUtil.getValueOr(soil, "dome_applied", "").equals("Y")) {
                        isDomeApplied = true;
                        break;
                    }
                }
                if (!isDomeApplied) {
                    for (HashMap wth : weathers) {
                        if (MapUtil.getValueOr(wth, "dome_applied", "").equals("Y")) {
                            isDomeApplied = true;
                            break;
                        }
                    }
                }
            }
        }

        return isDomeApplied;
    }

    public static void reviseData(HashMap data) {
        ArrayList<HashMap> wthArr = MapUtil.getObjectOr(data, "weathers", new ArrayList<HashMap>());
        HashMap<String, String> wstIdClimIdMap = new HashMap();
        for (HashMap wthData : wthArr) {
            wstIdClimIdMap.put(MapUtil.getValueOr(wthData, "wst_id", ""), MapUtil.getValueOr(wthData, "clim_id", ""));
        }
        ArrayList<HashMap> expArr = MapUtil.getObjectOr(data, "experiments", new ArrayList<HashMap>());
        for (HashMap expData : expArr) {
            ArrayList<HashMap<String, String>> events = MapUtil.getBucket(expData, "management").getDataList();
            boolean isFeExist = false;
            boolean isIrExist = false;
            for (HashMap<String, String> event : events) {
                String eventType = MapUtil.getValueOr(event, "event", "");
                if (isFeExist || eventType.equals("fertilizer")) {
                    isFeExist = true;
                } else if (isIrExist || eventType.equals("irrigation")) {
                    isIrExist = true;
                }
                if (isFeExist && isIrExist) {
                    break;
                }
            }
            if (isFeExist) {
                expData.put("FERTILIZER", "Y");
            }
            if (isIrExist) {
                expData.put("IRRIG", "Y");
            }
            String wst_id = MapUtil.getValueOr(expData, "wst_id", "");
            String clim_id = wstIdClimIdMap.get(wst_id);
            if (clim_id != null && !"".equals(clim_id)) {
                expData.put("clim_id", clim_id);
            }
        }
    }

    public static void generateId(HashMap data) {
        try {
            String json = toJSON(data);
            data.clear();
            AceDataset ace = AceParser.parse(json);
            ace.linkDataset();
            ArrayList<HashMap> arr;
            // Experiments
            arr = new ArrayList();
            for (AceExperiment exp : ace.getExperiments()) {
                HashMap expData = JSONAdapter.fromJSON(new String(exp.rebuildComponent()));
                arr.add(expData);
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
        } catch (IOException e) {
            LOG.warn(Functions.getStackTrace(e));
        }
    }
    
    public static void recordQuadUIVersion(HashMap map, String quaduiVer) {
        ArrayList<HashMap> exps = MapUtil.getObjectOr(map, "experiments", new ArrayList());
        for (HashMap exp : exps) {
            exp.put("quaduiVer", quaduiVer);
        }
    }

    public static HashMap<String, String> saveDomeHashedIds(HashMap map, HashMap<String, String> domeIdHashMap) {
        HashMap<String, String> ret = domeIdHashMap;
        if (domeIdHashMap == null) {
            ret = new HashMap();
            ret.putAll(loadDomeHashedIds(MapUtil.getObjectOr(map, "experiments", new ArrayList())));
            ret.putAll(loadDomeHashedIds(MapUtil.getObjectOr(map, "soils", new ArrayList())));
            ret.putAll(loadDomeHashedIds(MapUtil.getObjectOr(map, "weathers", new ArrayList())));
            if (ret.isEmpty()) {
                ret = null;
            }
        } else {
            saveDomeHashedIds(MapUtil.getObjectOr(map, "experiments", new ArrayList()), domeIdHashMap);
            saveDomeHashedIds(MapUtil.getObjectOr(map, "soils", new ArrayList()), domeIdHashMap);
            saveDomeHashedIds(MapUtil.getObjectOr(map, "weathers", new ArrayList()), domeIdHashMap);
        }

        return ret;
    }

    public static void saveDomeHashedIds(ArrayList<HashMap> arr, HashMap<String, String> domeIdHashMap) {

        for (HashMap data : arr) {
            if (MapUtil.getValueOr(data, "dome_applied", "").equals("Y")) {
                if (MapUtil.getValueOr(data, "seasonal_dome_applied", "").equals("Y")) {
                    String fieldName = MapUtil.getValueOr(data, "seasonal_strategy", "").toUpperCase();
                    String dsid = domeIdHashMap.get(fieldName);
                    if (dsid != null) {
                        data.put("dsid", dsid);
                    }
                }
                if (MapUtil.getValueOr(data, "rotational_dome_applied", "").equals("Y")) {
                    String fieldName = MapUtil.getValueOr(data, "rotational_strategy", "").toUpperCase();
                    String drid = domeIdHashMap.get(fieldName);
                    if (drid != null) {
                        data.put("drid", drid);
                    }
                }
                if (MapUtil.getValueOr(data, "field_dome_applied", "").equals("Y")) {
                    String fieldName = MapUtil.getValueOr(data, "field_overlay", "").toUpperCase();
                    String doid = domeIdHashMap.get(fieldName);
                    if (doid != null) {
                        data.put("doid", doid);
                    }
                }
                if (MapUtil.getValueOr(data, "batch_dome_applied", "").equals("Y")) {
                    String batchName = MapUtil.getValueOr(data, "batch_dome", "").toUpperCase();
                    String bdid = domeIdHashMap.get(batchName);
                    if (bdid != null) {
                        data.put("bdid", bdid);
                    }
                }
            }
        }
    }

    public static HashMap<String, String> loadDomeHashedIds(ArrayList<HashMap> arr) {

        HashMap<String, String> domeIdHashMap = new HashMap();
        for (HashMap data : arr) {
            String seasonalName = MapUtil.getValueOr(data, "seasonal_strategy", "").toUpperCase();
            String dsid = MapUtil.getValueOr(data, "dsid", "");
            if (!dsid.equals("") && !domeIdHashMap.containsKey(seasonalName)) {
                domeIdHashMap.put(seasonalName, dsid);
            }
            String rotationalName = MapUtil.getValueOr(data, "rotational_strategy", "").toUpperCase();
            String drid = MapUtil.getValueOr(data, "drid", "");
            if (!drid.equals("") && !domeIdHashMap.containsKey(rotationalName)) {
                domeIdHashMap.put(rotationalName, drid);
            }
            String fieldName = MapUtil.getValueOr(data, "field_overlay", "").toUpperCase();
            String doid = MapUtil.getValueOr(data, "doid", "");
            if (!doid.equals("") && !domeIdHashMap.containsKey(fieldName)) {
                domeIdHashMap.put(fieldName, doid);
            }
            String batchName = MapUtil.getValueOr(data, "batch_dome", "").toUpperCase();
            String bdid = MapUtil.getValueOr(data, "bdid", "");
            if (!bdid.equals("") && !domeIdHashMap.containsKey(batchName)) {
                domeIdHashMap.put(batchName, bdid);
            }
        }

        return domeIdHashMap;
    }

    public static void compressOutput(String outputDirectory, String model) throws IOException {
        File directory = new File(outputDirectory);
        File zipFile   = new File(directory, model+"_Input.zip");
        List<File> files   = Arrays.asList(directory.listFiles());
        FileOutputStream fos = new FileOutputStream(zipFile);
        ZipOutputStream  zos = new ZipOutputStream(fos);
        for(File f : files) {
            ZipEntry ze = new ZipEntry(f.getName());
            zos.putNextEntry(ze);
            zos.write(Files.toByteArray(f));
            zos.closeEntry();
            f.delete();
        }
        zos.close();
        fos.close();
    }
}
