package org.agmip.ui.quadui;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.agmip.common.Functions;
import static org.agmip.common.Functions.getStackTrace;
import org.agmip.dome.BatchEngine;
import org.agmip.translators.dssat.DssatControllerInput;
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

        DSSAT, APSIM, SarraHV33, STICS, WOFOST, CropGrowNAU, JSON
    }
    private static final Logger LOG = LoggerFactory.getLogger(QuadCmdLine.class);
    private DomeMode mode = DomeMode.NONE;
    private String convertPath = null;
    private String linkPath = null;
    private String fieldPath = null;
    private String strategyPath = null;
    private String batchPath = null;
    private String outputPath = null;
    private ArrayList<String> models = new ArrayList();
    private boolean helpFlg = false;
    private boolean isOverwrite = false;
    private boolean isCompressed = false;
    private boolean acebOnly = false;
    private boolean acebOnlyRet = true;
    private Properties versionProperties = new Properties();
    private String quadVersion = "";
    private boolean isFromCRAFT = false;
    private boolean isBatch = false;
    private int thrPoolSize = Runtime.getRuntime().availableProcessors();
    private HashMap modelSpecFiles;
    HashMap<String, Object> batch;
    private BatchEngine batEngine;
    private String outputDir = "";

    public QuadCmdLine() {
        try {
            InputStream versionFile = getClass().getClassLoader().getResourceAsStream("product.properties");
            versionProperties.load(versionFile);
            versionFile.close();
            StringBuilder qv = new StringBuilder();
            String buildType = versionProperties.getProperty("product.buildtype");
            qv.append("Version ");
            qv.append(versionProperties.getProperty("product.version"));
            qv.append("-").append(versionProperties.getProperty("product.buildversion"));
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
        modelSpecFiles = null;
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
            batch = null;
            batEngine = null;
            if (isBatch) {
                prepareBatchRun();
            } else {
                startTranslation();
            }
        } catch (Exception ex) {
            LOG.error(QuadUtil.getCurBatchInfo(batEngine, false) + getStackTrace(ex));
            if (ex.getClass().getSimpleName().equals("ZipException")) {
                LOG.error(QuadUtil.getCurBatchInfo(batEngine, false) + "Please make sure using the latest ADA (no earlier than 0.3.6) to create zip file");
            }
            runNextBatch(null);
        }

    }

    private void readCommand(String[] args) {
        int i = 0;
        int pathNum = 1;
        batchPath = null;
        isBatch = false;
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
            } else if (args[i].equalsIgnoreCase("-aceb")) {
                acebOnly = true;
            } else if (args[i].equalsIgnoreCase("-dssat")) {
                addModel(Model.DSSAT.toString());
            } else if (args[i].equalsIgnoreCase("-apsim")) {
                addModel(Model.APSIM.toString());
            } else if (args[i].equalsIgnoreCase("-sarrahv33")) {
                addModel(Model.SarraHV33.toString());
            } else if (args[i].equalsIgnoreCase("-stics")) {
                addModel(Model.STICS.toString());
            } else if (args[i].equalsIgnoreCase("-wofost")) {
                addModel(Model.WOFOST.toString());
            } else if (args[i].equalsIgnoreCase("-cropgrownau")) {
                addModel("CropGrow-NAU");
            } else if (args[i].equalsIgnoreCase("-agmip")) {
                addModel("AgMIP");
            } else if (args[i].equalsIgnoreCase("-json")) {
                addModel(Model.JSON.toString());
            } else if (args[i].equalsIgnoreCase("-cli")) {
            } else if (args[i].equalsIgnoreCase("-clean")) {
                isOverwrite = true;
            } else if (args[i].equalsIgnoreCase("-zip")) {
                isCompressed = true;
            } else if (args[i].equalsIgnoreCase("-thread")) {
                if (i < args.length - 1 && args[i + 1].matches("\\d+")) {
                    i++;
                    try {
                        thrPoolSize = Functions.numericStringToBigInteger(args[i]).intValue();
                    } catch (Exception e) {
                        LOG.warn("Invalid number for the size of thread pool, will use default value {}", thrPoolSize);
                    }
                } else {
                    LOG.warn("No valid number provided for the size of thread pool, will use default value {}", thrPoolSize);
                }
            } else if (args[i].equalsIgnoreCase("-batch")) {
                isBatch = true;
                pathNum++;
            } else {
                if (args[i].contains("D")) {
                    addModel(Model.DSSAT.toString());
                }
                if (args[i].contains("A")) {
                    addModel(Model.APSIM.toString());
                }
                if (args[i].contains("H")) {
                    addModel(Model.SarraHV33.toString());
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
                if (args[i].contains("G")) {
                    addModel("AgMIP");
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
                linkPath = "";
            }
            if (!isBatch) {
                if (pathNum >= 3) {
                    fieldPath = args[i++];
                }
                if (pathNum >= 4) {
                    strategyPath = args[i++];
                }
            } else {
                if (pathNum >= 3) {
                    if (!mode.equals(DomeMode.NONE)) {
                        fieldPath = args[i++];
                    } else {
                        batchPath = args[i++];
                    }
                }
                if (pathNum >= 4) {
                    if (mode.equals(DomeMode.STRATEGY)) {
                        strategyPath = args[i++];
                    } else if (batchPath == null) {
                        batchPath = args[i++];
                    }
                }
                if (batchPath == null && pathNum >= 5) {
                    batchPath = args[i++];
                }
            }

            if (i < args.length) {
                outputPath = args[i];
            } else {
                isFromCRAFT = new File(convertPath).isDirectory();
                try {
                    if (!isFromCRAFT) {
                        outputPath = new File(convertPath).getCanonicalFile().getParent();
                    } else {
                        outputPath = convertPath;
                    }
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

        if (!acebOnly && models.isEmpty()) {
            LOG.warn("<model_option> is required for running translation");
            return false;
        }

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

    private void prepareBatchRun() {
        LOG.info("Loading batch file...");
        this.batch = QuadUtil.loadBatchFile(batchPath);
        this.batEngine = new BatchEngine(this.batch);
//        QuadUtil.cleanBatchDir(outputPath, isOverwrite);
        if (batEngine.hasNext()) {
            startTranslation();
        }
    }

    private void startTranslation() {
        outputDir = QuadUtil.getOutputDir(outputPath, isOverwrite, batEngine);
        LOG.info(QuadUtil.getCurBatchInfo(batEngine, false) + "Importing data...");
        if (isFromCRAFT) {
            DssatControllerInput in = new DssatControllerInput();
            HashMap data = in.readFileFromCRAFT(convertPath);

            if (mode.equals(DomeMode.NONE)) {
                toOutput(data, null);
            } else {
                LOG.debug("Attempting to apply a new DOME");
                applyDome(data, mode.toString().toLowerCase(), new ArrayList());
            }
        } else {

            TranslateFromTask task;
            ArrayList<String> inputFiles = new ArrayList();
            inputFiles.add(convertPath);
            try {
                task = new TranslateFromTask(inputFiles.toArray(new String[0]));
                TaskListener<HashMap> listener = new TaskListener<HashMap>() {

                    @Override
                    public void taskExecuted(Task<HashMap> t) {
                        HashMap data = t.getResult();
                        if (!data.containsKey("errors")) {
                            modelSpecFiles = (HashMap) data.remove("ModelSpec");

                            // Dump input data into aceb format
                            boolean isDomeApplied = QuadUtil.isDomeApplied(convertPath.toLowerCase(), data);

                            if (!isDomeApplied) {
                                dumpToAceb(data);
                            }

                            if (batEngine != null) {
                                applyBatch(data);
                            } else {
                                if (mode.equals(DomeMode.NONE)) {
                                    if (!acebOnly) {
                                        toOutput(data, null);
                                    }
                                } else {
                                    applyDome(data, mode.toString().toLowerCase(), new ArrayList());
                                }
                            }

                        } else {
                            LOG.error((String) data.get("errors"));
                            runNextBatch(null);
                        }
                    }

                    @Override
                    public void executeFailed(Task<HashMap> arg0) {
                        LOG.error(getStackTrace(arg0.getFault()));
                        runNextBatch(arg0);
                    }
                };
                task.execute(new TaskAdapter<HashMap>(listener));
            } catch (Exception ex) {
                LOG.error(QuadUtil.getCurBatchInfo(batEngine, false) + getStackTrace(ex));
                if (ex.getClass().getSimpleName().equals("ZipException")) {
                    LOG.error(QuadUtil.getCurBatchInfo(batEngine, false) + "Please make sure using the latest ADA (no earlier than 0.3.6) to create zip file");
                }
                runNextBatch(null);
            }
        }
    }

    private void applyBatch(HashMap data) {

        // Apply batch DOME
        LOG.info(QuadUtil.getCurBatchInfo(batEngine, false) + "Applying batch [" + batEngine.getBatchName() + "]...");

        ApplyBatchTask task = new ApplyBatchTask(data, batEngine);
        TaskListener<HashMap> listener = new TaskListener<HashMap>() {

            @Override
            public void taskExecuted(Task<HashMap> task) {
                HashMap data = task.getResult();
                if (mode.equals(DomeMode.NONE)) {
                    if (!acebOnly) {
                        toOutput(data, null);
                    }
                } else {
                    applyDome(data, mode.toString().toLowerCase(), batEngine.currentModifiedVarList());
                }
            }

            @Override
            public void executeFailed(Task<HashMap> task) {
                LOG.error(QuadUtil.getCurBatchInfo(batEngine, false) + getStackTrace(task.getFault()));
                runNextBatch(task);
            }
        };
        task.execute(new TaskAdapter<HashMap>(listener));
    }

    private void dumpToAceb(HashMap map) {
        dumpToAceb(map, false);
    }

    private void dumpToAceb(HashMap map, final boolean isDome) {

        if (!isDome) {
            QuadUtil.generateId(map);
        }
        String filePath;
        if (!isDome) {
            filePath = convertPath;
        } else if (mode.equals(DomeMode.STRATEGY)) {
            filePath = strategyPath;
        } else if (mode.equals(DomeMode.FIELD)) {
            filePath = fieldPath;
        } else {
            filePath = convertPath;
        }
        String filePathL;
        if (linkPath == null || linkPath.trim().equals("")) {
            filePathL = convertPath;
        } else {
            filePathL = linkPath;
        }
        final String fileName = new File(convertPath).getName();
        final HashMap result = (HashMap) map.get("domeoutput");
        boolean isSkipped = false;
        boolean isSkippedForLink = false;
        if (!isDome && convertPath.toUpperCase().endsWith(".ACEB")) {
            return;
        } else if (isDome
                && (fieldPath.toUpperCase().endsWith(".JSON") || fieldPath.toUpperCase().endsWith(".DOME"))
                && (mode.equals(DomeMode.STRATEGY) && (strategyPath.toUpperCase().endsWith(".JSON") || strategyPath.toUpperCase().endsWith(".DOME")))) {
            isSkipped = true;
        }
        if (linkPath != null && (linkPath.toUpperCase().endsWith(".ACEB") || linkPath.toUpperCase().endsWith(".ALNK"))) {
            isSkippedForLink = true;
        }
        if (isSkipped) {
            LOG.info("Skip generating ACE Binary file for DOMEs applied for {} ...", fileName);
        } else if (isDome) {
            LOG.info("Generate ACE Binary file for DOMEs applied for {} ...", fileName);
        } else {
            LOG.info("Generate ACE Binary file for {} ...", fileName);
        }
        if (isSkippedForLink) {
            LOG.info("Skip generating ACE Binary file for linkage information used for {} ...", fileName);
        }
        DumpToAceb task = new DumpToAceb(filePath, filePathL, outputPath, map, isDome, isSkipped, isSkippedForLink);
        TaskListener<HashMap<String, String>> listener = new TaskListener<HashMap<String, String>>() {
            @Override
            public void taskExecuted(Task<HashMap<String, String>> t) {
                if (isDome) {
                    LOG.info("Dump to ACE Binary for DOMEs applied for {} successfully", fileName);
                } else {
                    LOG.info("Dump to ACE Binary for {} successfully", fileName);
                }
                if (acebOnly) {
                    toOutput(result, t.getResult());
                } else if (isDome) {
                    QuadUtil.reviseData(result);
                    toOutput(result, t.getResult());
                }
            }

            @Override
            public void executeFailed(Task<HashMap<String, String>> arg0) {
                if (isDome) {
                    LOG.info("Dump to ACE Binary for DOMEs applied for {} failed", fileName);
                } else {
                    LOG.info("Dump to ACE Binary for {} failed", fileName);
                }
                LOG.error(getStackTrace(arg0.getFault()));
                if (acebOnly) {
                    acebOnlyRet = false;
                    toOutput(result, arg0.getResult());
                } else if (isDome) {
                    QuadUtil.reviseData(result);
                    toOutput(result, arg0.getResult());
                }
            }
        };
        task.execute(new TaskAdapter<HashMap<String, String>>(listener));
    }

    private void applyDome(HashMap map, String mode, ArrayList<String> skipVarList) {
        LOG.info("Applying DOME...");
        ApplyDomeTask task = new ApplyDomeTask(linkPath, fieldPath, strategyPath, mode, map, skipVarList, isAutoDomeApply(), acebOnly, thrPoolSize);
        final HashMap batchDome = this.batch;
        TaskListener<HashMap> listener = new TaskListener<HashMap>() {
            @Override
            public void taskExecuted(Task<HashMap> t) {
                HashMap data = t.getResult();
                if (!data.containsKey("errors")) {
                    //LOG.error("Domeoutput: {}", data.get("domeoutput"));
                    if (batEngine != null) {
                        HashMap tmp = new HashMap();
                        tmp.put(batEngine.getDomeName(), batchDome);
                        data.put("batDomes", tmp);
                    }
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

        if (acebOnly) {
            if (acebOnlyRet) {
                LOG.info("=== Completed translation job ===");
                return;
            }
        }
        domeIdHashMap = QuadUtil.saveDomeHashedIds(map, domeIdHashMap);
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
            DumpToJson task = new DumpToJson(convertPath, outputDir, map);
            TaskListener<String> listener = new TaskListener<String>() {
                @Override
                public void taskExecuted(Task<String> t) {
                    LOG.info("Translation completed");
                    runNextBatch(t);
                }

                @Override
                public void executeFailed(Task<String> arg0) {
                    LOG.error(getStackTrace(arg0.getFault()));
                    runNextBatch(arg0);
                }
            };
            task.execute(new TaskAdapter<String>(listener));
        } else {
            if (models.indexOf("JSON") != -1) {
                DumpToJson task = new DumpToJson(convertPath, outputDir, map);
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
        TranslateToTask task = new TranslateToTask(models, map, outputDir, isCompressed, domeIdHashMap, modelSpecFiles);
        TaskListener<String> listener = new TaskListener<String>() {
            @Override
            public void executeFailed(Task<String> arg0) {
                LOG.error(getStackTrace(arg0.getFault()));
                runNextBatch(arg0);
            }

            @Override
            public void taskExecuted(Task<String> arg0) {
                LOG.info("=== Completed translation job ===");
                runNextBatch(arg0);
            }
        };
        task.execute(new TaskAdapter<String>(listener));
    }

    private void runNextBatch(Task arg0) {
        if (batEngine != null && batEngine.hasNext()) {
            LOG.info("=== Batch " + batEngine.getNextGroupId() + " finished ===");
            startTranslation();
        } else if (arg0 != null) {
            arg0.abort();
        }
    }

    private void printHelp() {
        System.out.println("\nThe arguments format : <option_compress> <option_overwrite> <multi_thread_option> <dome_mode_option> <model_option> <batch_option> <convert_path> <link_path> <field_path> <strategy_path> <batch_path> <output_path>");
//            System.out.println("\nThe arguments format : <dome_mode_option> <model_option> <convert_path> <field_path> <strategy_path> <output_path>");
        System.out.println("\t<option_compress>");
        System.out.println("\t\t-zip \tCompress the generated file into zip package.");
        System.out.println("\t\t\tIf not provide, no compression will be done");
        System.out.println("\t<option_overwrite>");
        System.out.println("\t\t-clean \tClean model folder under the selected path.");
        System.out.println("\t\t\tIf not provide, will choose new folder for particular model if the folder is filled with files.");
        System.out.println("\t<multi_thread_option>");
        System.out.println("\t\t-thread [thread pool size] \tAssign the pool size for multi-thread running.");
        System.out.println("\t\t\tIf not provide, will auto-detect the number of operator cores and use that number as pool size.");
        System.out.println("\t\t\tIf provide 1, will choose to use single thread mode");
        System.out.println("\t\t\tIf provide -1, will choose to use cached thread pool with flexible pool size");
        System.out.println("\t<dome_mode_option>");
        System.out.println("\t\t-n | -none\tRaw Data Only, Default");
        System.out.println("\t\t-f | -filed\tField Overlay, will require Field Overlay File");
        System.out.println("\t\t-s | -strategy\tSeasonal Strategy, will require both Field Overlay and Strategy File");
        System.out.println("\t<model_option>");
        System.out.println("\t\t-D | -dssat\tDSSAT");
        System.out.println("\t\t-A | -apsim\tAPSIM");
        System.out.println("\t\t-H | -sarrahv33\tSarraHV33");
        System.out.println("\t\t-S | -stics\tSTICS");
        System.out.println("\t\t-W | -wofost\tWOFOST");
        System.out.println("\t\t-C | -cropgrownau\tCropGrow-NAU");
        System.out.println("\t\t-G | -agmip\tAgMIP Weather");
        System.out.println("\t\t-J | -json\tJSON");
        System.out.println("\t\t-aceb\t\tACEB only, will ignore other model choices");
        System.out.println("\t\t* Could be combined input like -DAJ or -DJ");
        System.out.println("\t<option_batch>");
        System.out.println("\t\t-batch \tWill use provided batch DOME file to run multiple translation at one time.");
        System.out.println("\t<convert_path>");
        System.out.println("\t\tThe path for file to be converted");
        System.out.println("\t<link_path>");
        System.out.println("\t\tThe path for file to be used for link dome command to data set");
        System.out.println("\t\tIf not intend to provide link file, please set it as \"\"");
        System.out.println("\t<field_path>");
        System.out.println("\t\tThe path for file to be used for field overlay");
        System.out.println("\t<strategy_path>");
        System.out.println("\t\tThe path for file to be used for strategy");
        System.out.println("\t<batch_path>");
        System.out.println("\t\tThe path for file to be used for batch run");
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
        if (isBatch) {
            LOG.info("batchPath:\t" + batchPath);
        }
        LOG.info("outputPath:\t" + outputPath);
        if (acebOnly) {
            LOG.info("Models:\t\tACEB only");
        } else {
            LOG.info("Models:\t\t" + models);
        }
        LOG.info("Thread pool size: \t" + thrPoolSize);
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
