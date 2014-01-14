package org.agmip.ui.quadui;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceSoil;
import org.agmip.ace.AceWeather;
import org.agmip.ace.io.AceGenerator;
import org.agmip.ace.io.AceParser;
import org.agmip.common.Functions;
import static org.agmip.common.Functions.getStackTrace;
import org.agmip.translators.dssat.DssatControllerInput;
import org.agmip.util.JSONAdapter;
import static org.agmip.util.JSONAdapter.*;
import org.agmip.util.MapUtil;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskListener;
import org.apache.pivot.wtk.TaskAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Meng Zhang
 */
public class QuadCmdLine {

    public enum DomeMode {

        NONE, FIELD, STRATEGY
    }

    public enum Model {

        DSSAT, APSIM, STICS, WOFOST, CropGrowNAU, JSON
    }
    private static Logger LOG = LoggerFactory.getLogger(QuadCmdLine.class);
    private DomeMode mode = DomeMode.NONE;
    private String convertPath = null;
    private String linkPath = null;
    private String fieldPath = null;
    private String strategyPath = null;
    private String outputPath = null;
    private ArrayList<String> models = new ArrayList();
    private boolean helpFlg = false;
    private boolean isOverwrite = false;
    private boolean isCompressed = false;
    private Properties versionProperties = new Properties();
    private String quadVersion = "";
    private boolean isFromCRAFT = false;

    public QuadCmdLine() {
        try {
            InputStream versionFile = getClass().getClassLoader().getResourceAsStream("product.properties");
            versionProperties.load(versionFile);
            versionFile.close();
            StringBuilder qv = new StringBuilder();
            String buildType = versionProperties.getProperty("product.buildtype").toString();
            qv.append("Version ");
            qv.append(versionProperties.getProperty("product.version").toString());
            qv.append("-").append(versionProperties.getProperty("product.buildversion").toString());
            qv.append("(").append(buildType).append(")");
            if (buildType.equals("dev")) {
                qv.append(" [").append(versionProperties.getProperty("product.buildts")).append("]");
            }
            quadVersion = qv.toString();
        } catch (IOException ex) {
            LOG.error("Unable to load version information, version will be blank.");
        }
    }

    public void run(String[] args) {

        LOG.info("QuadUI {} lauched with JAVA {} under OS {}", quadVersion, System.getProperty("java.runtime.version"), System.getProperty("os.name"));
        readCommand(args);
        if (helpFlg) {
            printHelp();
            return;
        } else if (!validate()) {
            LOG.info("Type -h or -help for arguments info");
            return;
        } else {
            argsInfo();
        }

        LOG.info("Starting translation job");
        try {
            startTranslation();
        } catch (Exception ex) {
            LOG.error(getStackTrace(ex));
        }

    }

    private void readCommand(String[] args) {
        int i = 0;
        int pathNum = 1;
        while (i < args.length && args[i].startsWith("-")) {
            if (args[i].equalsIgnoreCase("-n") || args[i].equalsIgnoreCase("-none")) {
                mode = DomeMode.NONE;
                pathNum = 1;
            } else if (args[i].equalsIgnoreCase("-f") || args[i].equalsIgnoreCase("-field")) {
                mode = DomeMode.FIELD;
                pathNum = 3;
            } else if (args[i].equalsIgnoreCase("-s") || args[i].equalsIgnoreCase("-strategy")) {
                mode = DomeMode.STRATEGY;
                pathNum = 4;
            } else if (args[i].equalsIgnoreCase("-h") || args[i].equalsIgnoreCase("-help")) {
                helpFlg = true;
                return;
            } else if (args[i].equalsIgnoreCase("-dssat")) {
                addModel(Model.DSSAT.toString());
            } else if (args[i].equalsIgnoreCase("-apsim")) {
                addModel(Model.APSIM.toString());
            } else if (args[i].equalsIgnoreCase("-stics")) {
                addModel(Model.STICS.toString());
            } else if (args[i].equalsIgnoreCase("-wofost")) {
                addModel(Model.WOFOST.toString());
            } else if (args[i].equalsIgnoreCase("-cropgrownau")) {
                addModel("CropGrow-NAU");
            } else if (args[i].equalsIgnoreCase("-json")) {
                addModel(Model.JSON.toString());
            } else if (args[i].equalsIgnoreCase("-cli")) {
            } else if (args[i].equalsIgnoreCase("-clean")) {
                isOverwrite = true;
            } else if (args[i].equalsIgnoreCase("-zip")) {
                isCompressed = true;
            } else {
                if (args[i].contains("D")) {
                    addModel(Model.DSSAT.toString());
                }
                if (args[i].contains("A")) {
                    addModel(Model.APSIM.toString());
                }
                if (args[i].contains("S")) {
                    addModel(Model.STICS.toString());
                }
                if (args[i].contains("W")) {
                    addModel(Model.WOFOST.toString());
                }
                if (args[i].contains("C")) {
                    addModel("CropGrow-NAU");
                }
                if (args[i].contains("J")) {
                    addModel(Model.JSON.toString());
                }
            }
            i++;
        }
        try {
            if (pathNum >= 1) {
                convertPath = args[i++];
            }
            if (pathNum >= 2) {
                linkPath = args[i++].trim();
            }
            if (pathNum >= 3) {
                fieldPath = args[i++];
            }
            if (pathNum >= 4) {
                strategyPath = args[i++];
            }
            if (i < args.length) {
                outputPath = args[i];
            } else {
                try {
                    outputPath = new File(convertPath).getCanonicalFile().getParent();
                } catch (IOException ex) {
                    outputPath = null;
                    LOG.error(getStackTrace(ex));
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("Path arguments are not enough for selected dome mode");
        }
    }

    private void addModel(String model) {
        if (models.indexOf(model) < 0) {
            models.add(model);
        }
    }

    private boolean validate() {

        if (convertPath == null || !new File(convertPath).exists()) {
            LOG.warn("convert_path is invalid : " + convertPath);
            return false;
        } else if (!isValidPath(outputPath, false)) {
            LOG.warn("output_path is invalid : " + outputPath);
            return false;
        }

        if (mode.equals(DomeMode.NONE)) {
        } else if (mode.equals(DomeMode.FIELD)) {
            if (!linkPath.equals("") && !isValidPath(linkPath, true)) {
                LOG.warn("link_path is invalid : " + linkPath);
                return false;
            } else if (!isValidPath(fieldPath, true)) {
                LOG.warn("field_path is invalid : " + fieldPath);
                return false;
            }
        } else if (mode.equals(DomeMode.STRATEGY)) {
            if (!linkPath.equals("") && !isValidPath(linkPath, true)) {
                LOG.warn("link_path is invalid : " + linkPath);
                return false;
            } else if (!isValidPath(fieldPath, true)) {
                LOG.warn("field_path is invalid : " + fieldPath);
                return false;
            } else if (!isValidPath(strategyPath, true)) {
                LOG.warn("strategy_path is invalid : " + strategyPath);
                return false;
            }
        } else {
            LOG.warn("Unsupported mode option : " + mode);
            return false;
        }

        if (models.isEmpty()) {
            LOG.warn("<model_option> is required for running translation");
            return false;
        }
        
        isFromCRAFT = new File(convertPath).isDirectory();
        if (outputPath != null) {
            File dir = new File(outputPath);
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }

        return true;
    }

    private boolean isValidPath(String path, boolean isFile) {
        if (path == null) {
            return false;
        } else {
            File f = new File(path);
            if (isFile) {
                return f.isFile();
            } else {
                return !path.matches(".*\\.\\w+$");
            }
        }
    }

    private void startTranslation() throws Exception {
        LOG.info("Importing data...");
        if (isFromCRAFT) {
            DssatControllerInput in = new DssatControllerInput();
            HashMap data = in.readFileFromCRAFT(convertPath);
            
            if (mode.equals(DomeMode.NONE)) {
                toOutput(data, null);
            } else {
                LOG.debug("Attempting to apply a new DOME");
                applyDome(data, mode.toString().toLowerCase());
            }
        } else if (convertPath.endsWith(".json")) {
            try {
                // Load the JSON representation into memory and send it down the line.
                String json = new Scanner(new File(convertPath), "UTF-8").useDelimiter("\\A").next();
                HashMap data = fromJSON(json);

                // Check if the data has been applied with DOME.
                boolean isDomeApplied = false;
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
                // If it has not been applied with DOME, then dump to ACEB
                if (!isDomeApplied) {
                    dumpToAceb(data);
                }
                if (mode.equals(DomeMode.NONE)) {
                    toOutput(data, null);
                } else {
                    LOG.debug("Attempting to apply a new DOME");
                    applyDome(data, mode.toString().toLowerCase());
                }
            } catch (Exception ex) {
                LOG.error(getStackTrace(ex));
            }
        } else if (convertPath.endsWith(".aceb")) {
            try {
                // Load the ACE Binay file into memory and transform it to old JSON format and send it down the line.
                AceDataset ace = AceParser.parseACEB(new File(convertPath));
                ace.linkDataset();
//                ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                AceGenerator.generate(baos, acebData, false);
//                HashMap data = fromJSON(baos.toString());
//                baos.close();
                
                HashMap data = new HashMap();
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
                ace = null;

                if (mode.equals(DomeMode.NONE)) {
                    toOutput(data, null);
                } else {
                    LOG.debug("Attempting to apply a new DOME");
                    applyDome(data, mode.toString().toLowerCase());
                }
            } catch (Exception ex) {
                LOG.error(getStackTrace(ex));
            }
        } else {
            TranslateFromTask task = new TranslateFromTask(convertPath);
            TaskListener<HashMap> listener = new TaskListener<HashMap>() {
                @Override
                public void taskExecuted(Task<HashMap> t) {
                    HashMap data = t.getResult();
                    if (!data.containsKey("errors")) {
                        dumpToAceb(data);
                        if (mode.equals(DomeMode.NONE)) {
                            toOutput(data, null);
                        } else {
                            applyDome(data, mode.toString().toLowerCase());
                        }
                    } else {
                        LOG.error((String) data.get("errors"));
                    }
                    t.abort();
                }

                @Override
                public void executeFailed(Task<HashMap> arg0) {
                    LOG.error(getStackTrace(arg0.getFault()));
                    arg0.abort();
                }
            };
            task.execute(new TaskAdapter<HashMap>(listener));
        }
    }

    private void dumpToAceb(HashMap map) {
        dumpToAceb(map, false);
    }

    private void dumpToAceb(HashMap map, final boolean isDome) {

        if (!isDome) {
            generateId(map);
        }
        final String fileName = new File(convertPath).getName();
        final HashMap result = (HashMap) map.get("domeoutput");
        boolean isSkipped = false;
        boolean isSkippedForLink = false;
        if (map == null || (!isDome && convertPath.toUpperCase().endsWith(".ACEB"))) {
            return;
        } else if (isDome && fieldPath.toUpperCase().endsWith(".ACEB") && strategyPath.toUpperCase().endsWith(".ACEB")) {
            isSkipped = true;
        }
        if (linkPath != null && linkPath.toUpperCase().endsWith(".ACEB")) {
            isSkippedForLink = true;
        }
        if (isSkipped) {
            LOG.info("Skip generating ACE Baniry file for DOMEs applied for {} ...", fileName);
        } else if (isDome) {
            LOG.info("Generate ACE Baniry file for DOMEs applied for {} ...", fileName);
        } else {
            LOG.info("Generate ACE Baniry file for {} ...", fileName);
        }
        if (isSkippedForLink) {
            LOG.info("Skip generating ACE Baniry file for linkage information used for {} ...", fileName);
        }
        DumpToAceb task = new DumpToAceb(convertPath, outputPath, map, isDome, isSkipped, isSkippedForLink);
        TaskListener<HashMap<String, String>> listener = new TaskListener<HashMap<String, String>>() {
            @Override
            public void taskExecuted(Task<HashMap<String, String>> t) {
                LOG.info("Dump to ACE Binary for {} successfully", fileName);
                if (isDome) {
                    toOutput(result, t.getResult());
                }
            }

            @Override
            public void executeFailed(Task<HashMap<String, String>> arg0) {
                LOG.info("Dump to ACE Binary for {} failed", fileName);
                LOG.error(getStackTrace(arg0.getFault()));
                if (isDome) {
                    toOutput(result, arg0.getResult());
                }
            }
        };
        task.execute(new TaskAdapter<HashMap<String, String>>(listener));
    }

    private void generateId(HashMap data) {
        try {
            String json = toJSON(data);
            data.clear();
            AceDataset ace = AceParser.parse(json);
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
        } catch (IOException e) {
            LOG.warn(getStackTrace(e));
        }
    }

    private void applyDome(HashMap map, String mode) {
        LOG.info("Applying DOME...");
        ApplyDomeTask task = new ApplyDomeTask(linkPath, fieldPath, strategyPath, mode, map, isAutoDomeApply());
        TaskListener<HashMap> listener = new TaskListener<HashMap>() {
            @Override
            public void taskExecuted(Task<HashMap> t) {
                HashMap data = t.getResult();
                if (!data.containsKey("errors")) {
                    //LOG.error("Domeoutput: {}", data.get("domeoutput"));
                    dumpToAceb(data, true);
                } else {
                    LOG.error((String) data.get("errors"));
                }
                t.abort();
            }

            @Override
            public void executeFailed(Task<HashMap> arg0) {
                LOG.error(getStackTrace(arg0.getFault()));
                arg0.abort();
            }
        };
        task.execute(new TaskAdapter<HashMap>(listener));
    }

    private void toOutput(HashMap map, HashMap<String, String> domeIdHashMap) {

        if (isOverwrite) {
            LOG.info("Clean the previous output folders...");
            String outPath;
            if (outputPath.equals(File.separator)) {
                outPath = outputPath;
            } else {
                outPath = outputPath + File.separator;
            }

            for (String model : models) {
                if (model.equalsIgnoreCase("JSON")) {
                    continue;
                }
                File dir = new File(outPath + model);
                if (!Functions.clearDirectory(dir)) {
                    LOG.warn("Failed to clean {} folder since it is being used by other process", model);
                }
            }
        }
        LOG.info("Generating model input files...");

        if (models.size() == 1 && models.get(0).equals("JSON")) {
            DumpToJson task = new DumpToJson(convertPath, outputPath, map);
            TaskListener<String> listener = new TaskListener<String>() {
                @Override
                public void taskExecuted(Task<String> t) {
                    LOG.info("Translation completed");
                    t.abort();
                }

                @Override
                public void executeFailed(Task<String> arg0) {
                    LOG.error(getStackTrace(arg0.getFault()));
                    arg0.abort();
                }
            };
            task.execute(new TaskAdapter<String>(listener));
        } else {
            if (models.indexOf("JSON") != -1) {
                DumpToJson task = new DumpToJson(convertPath, outputPath, map);
                TaskListener<String> listener = new TaskListener<String>() {
                    @Override
                    public void taskExecuted(Task<String> t) {
//                        toOutput2(t.getResult());
                        t.abort();
                    }

                    @Override
                    public void executeFailed(Task<String> arg0) {
                        LOG.error(getStackTrace(arg0.getFault()));
                        arg0.abort();
                    }
                };
                task.execute(new TaskAdapter<String>(listener));
            }
            toOutput2(map, domeIdHashMap);
        }
    }

    private void toOutput2(HashMap map, HashMap<String, String> domeIdHashMap) {
        TranslateToTask task = new TranslateToTask(models, map, outputPath, isCompressed, domeIdHashMap);
        TaskListener<String> listener = new TaskListener<String>() {
            @Override
            public void executeFailed(Task<String> arg0) {
                LOG.error(getStackTrace(arg0.getFault()));
                arg0.abort();
            }

            @Override
            public void taskExecuted(Task<String> arg0) {
                LOG.info("=== Completed translation job ===");
                arg0.abort();
            }
        };
        task.execute(new TaskAdapter<String>(listener));
    }

    private void printHelp() {
        System.out.println("\nThe arguments format : <option_compress> <option_overwrite> <dome_mode_option> <model_option> <convert_path> <link_path> <field_path> <strategy_path> <output_path>");
//            System.out.println("\nThe arguments format : <dome_mode_option> <model_option> <convert_path> <field_path> <strategy_path> <output_path>");
        System.out.println("\t<option_compress>");
        System.out.println("\t\t-zip \tCompress the generated file into zip package.");
        System.out.println("\t\t\tIf not provide, no compression will be done");
        System.out.println("\t<option_overwrite>");
        System.out.println("\t\t-clean \tClean model folder under the selected path.");
        System.out.println("\t\t\tIf not provide, will choose new folder for particular model if the folder is filled with files.");
        System.out.println("\t<dome_mode_option>");
        System.out.println("\t\t-n | -none\tRaw Data Only, Default");
        System.out.println("\t\t-f | -filed\tField Overlay, will require Field Overlay File");
        System.out.println("\t\t-s | -strategy\tSeasonal Strategy, will require both Field Overlay and Strategy File");
        System.out.println("\t<model_option>");
        System.out.println("\t\t-D | -dssat\tDSSAT");
        System.out.println("\t\t-A | -apsim\tAPSIM");
        System.out.println("\t\t-S | -stics\tSTICS");
        System.out.println("\t\t-W | -wofost\tWOFOST");
        System.out.println("\t\t-C | -cropgrownau\tCropGrow-NAU");
        System.out.println("\t\t-J | -json\tJSON");
        System.out.println("\t\t* Could be combined input like -DAJ or -DJ");
        System.out.println("\t<convert_path>");
        System.out.println("\t\tThe path for file to be converted");
        System.out.println("\t<link_path>");
        System.out.println("\t\tThe path for file to be used for link dome command to data set");
        System.out.println("\t\tIf not intend to provide link file, please set it as \"\"");
        System.out.println("\t<field_path>");
        System.out.println("\t\tThe path for file to be used for field overlay");
        System.out.println("\t<strategy_path>");
        System.out.println("\t\tThe path for file to be used for strategy");
        System.out.println("\t<output_path>");
        System.out.println("\t\tThe path for output.");
        System.out.println("\t\t* If not provided, will use convert_path");
        System.out.println("\n");
    }

    private void argsInfo() {
        LOG.info("Dome mode: \t" + mode);
        LOG.info("convertPath:\t" + convertPath);
        LOG.info("linkPath: \t" + linkPath);
        LOG.info("fieldPath: \t" + fieldPath);
        LOG.info("strategyPath:\t" + strategyPath);
        LOG.info("outputPath:\t" + outputPath);
        LOG.info("Models:\t\t" + models);
    }

    private boolean isAutoDomeApply() {
        File convertFile = new File(convertPath);
        String fileName = convertFile.getName().toLowerCase();
        boolean autoApply = false;
        if (!linkPath.equals("")) {
            autoApply = false;
        } else if (isFromCRAFT) {
            autoApply = true;
        } else if (fileName.endsWith(".zip")) {
            try {
                ZipFile zf = new ZipFile(convertFile);
                Enumeration<? extends ZipEntry> e = zf.entries();
                while (e.hasMoreElements()) {
                    ZipEntry ze = (ZipEntry) e.nextElement();
                    String zeName = ze.getName().toLowerCase();
                    if (!zeName.endsWith(".csv")) {
                        autoApply = true;
                    } else {
                        autoApply = false;
                        break;
                    }
                }
                zf.close();
            } catch (IOException ex) {
            }

        } else if (!fileName.endsWith(".csv")) {
            autoApply = true;
        }
        return autoApply;
    }
}
