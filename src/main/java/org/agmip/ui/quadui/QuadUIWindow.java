package org.agmip.ui.quadui;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.util.prefs.Preferences;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceSoil;
import org.agmip.ace.AceWeather;
import org.agmip.ace.io.AceParser;
import org.agmip.common.Functions;
import org.agmip.util.JSONAdapter;
import static org.agmip.util.JSONAdapter.*;
import org.agmip.util.MapUtil;
import org.apache.pivot.beans.Bindable;
import org.apache.pivot.collections.Map;
import org.apache.pivot.util.Filter;
import org.apache.pivot.util.Resources;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskListener;
import org.apache.pivot.wtk.Action;
import org.apache.pivot.wtk.ActivityIndicator;
import org.apache.pivot.wtk.Alert;
import org.apache.pivot.wtk.BoxPane;
import org.apache.pivot.wtk.Button;
import org.apache.pivot.wtk.Button.State;
import org.apache.pivot.wtk.ButtonGroup;
import org.apache.pivot.wtk.ButtonGroupListener;
import org.apache.pivot.wtk.ButtonPressListener;
import org.apache.pivot.wtk.ButtonStateListener;
import org.apache.pivot.wtk.Checkbox;
import org.apache.pivot.wtk.Component;
import org.apache.pivot.wtk.DesktopApplicationContext;
import org.apache.pivot.wtk.FileBrowserSheet;
import org.apache.pivot.wtk.Label;
import org.apache.pivot.wtk.MessageType;
import org.apache.pivot.wtk.Orientation;
import org.apache.pivot.wtk.PushButton;
import org.apache.pivot.wtk.Sheet;
import org.apache.pivot.wtk.SheetCloseListener;
import org.apache.pivot.wtk.TaskAdapter;
import org.apache.pivot.wtk.TextInput;
import org.apache.pivot.wtk.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuadUIWindow extends Window implements Bindable {
    private static Logger LOG = LoggerFactory.getLogger(QuadUIWindow.class);
    private ActivityIndicator convertIndicator = null;
    private PushButton convertButton = null;
    private PushButton browseToConvert = null;
    private PushButton browseOutputDir = null;
    private PushButton browseLinkFile = null;
    private PushButton browseFieldFile = null;
    private PushButton browseStrategyFile = null;
    private ButtonGroup runType = null;
    private Checkbox modelApsim = null;
    private Checkbox modelDssat = null;
    private Checkbox modelStics = null;
    private Checkbox modelWofost = null;
    private Checkbox modelCgnau = null;
    private Checkbox modelJson = null;
    private Checkbox optionCompress = null;
    private Checkbox optionOverwrite = null;
    private Checkbox optionLinkage = null;
    private Label txtStatus = null;
    private Label txtAutoDomeApplyMsg = null;
    private Label txtVersion = null;
    private Label lblLink = null;
    private Label lblField = null;
    private Label lblStrategy = null;
    private TextInput outputText = null;
    private TextInput convertText = null;
    private TextInput linkText = null;
    private TextInput fieldText = null;
    private TextInput strategyText = null;
    private BoxPane linkBP = null;
    private ArrayList<Checkbox> checkboxGroup = new ArrayList<Checkbox>();
    private ArrayList<String> errors = new ArrayList<String>();
    private Properties versionProperties = new Properties();
    private String quadVersion = "";
    private Preferences pref = Preferences.userNodeForPackage(getClass());
    private String mode = "";
    private boolean autoApply = false;
    private HashMap modelSpecFiles;

    public QuadUIWindow() {
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

        Action.getNamedActions().put("fileQuit", new Action() {
            @Override
            public void perform(Component src) {
                DesktopApplicationContext.exit();
            }
        });
    }

    private ArrayList<String> validateInputs() {
        ArrayList<String> errors = new ArrayList<String>();
        boolean anyModelChecked = false;
        for (Checkbox cbox : checkboxGroup) {
            if (cbox.isSelected()) {
                anyModelChecked = true;
            }
        }
        if (!anyModelChecked) {
            errors.add("You need to select an output format");
        }
        File convertFile = new File(convertText.getText());
        File outputDir = new File(outputText.getText());
        if (!convertFile.exists()) {
            errors.add("You need to select a file to convert");
        }
        if (!outputDir.exists() || !outputDir.isDirectory()) {
            errors.add("You need to select an output directory");
        }
        return errors;
    }

    @Override
    public void initialize(Map<String, Object> ns, URL location, Resources res) {
        convertIndicator    = (ActivityIndicator) ns.get("convertIndicator");
        convertButton       = (PushButton) ns.get("convertButton");
        browseToConvert     = (PushButton) ns.get("browseConvertButton");
        browseOutputDir     = (PushButton) ns.get("browseOutputButton");
        browseLinkFile     = (PushButton) ns.get("browseLinkButton");
        browseFieldFile     = (PushButton) ns.get("browseFieldButton");
        browseStrategyFile  = (PushButton) ns.get("browseStrategyButton");
        runType             = (ButtonGroup) ns.get("runTypeButtons");
        txtStatus           = (Label) ns.get("txtStatus");
        txtAutoDomeApplyMsg = (Label) ns.get("txtAutoDomeApplyMsg");
        txtVersion          = (Label) ns.get("txtVersion");
        lblLink            = (Label) ns.get("linkLabel");
        lblField            = (Label) ns.get("fieldLabel");
        lblStrategy         = (Label) ns.get("strategyLabel");
        linkBP              = (BoxPane) ns.get("linkBP");
        convertText         = (TextInput) ns.get("convertText");
        outputText          = (TextInput) ns.get("outputText");
        linkText           = (TextInput) ns.get("linkText");
        fieldText           = (TextInput) ns.get("fieldText");
        strategyText        = (TextInput) ns.get("strategyText");
        modelApsim          = (Checkbox) ns.get("model-apsim");
        modelDssat          = (Checkbox) ns.get("model-dssat");
        modelStics          = (Checkbox) ns.get("model-stics");
        modelWofost         = (Checkbox) ns.get("model-wofost");
        modelCgnau          = (Checkbox) ns.get("model-cgnau");
        modelJson           = (Checkbox) ns.get("model-json");
        optionCompress      = (Checkbox) ns.get("option-compress");
        optionOverwrite      = (Checkbox) ns.get("option-overwrite");
        optionLinkage      = (Checkbox) ns.get("option-linkage");

        checkboxGroup.add(modelApsim);
        checkboxGroup.add(modelDssat);
        checkboxGroup.add(modelStics);
        checkboxGroup.add(modelWofost);
        checkboxGroup.add(modelCgnau);
        checkboxGroup.add(modelJson);

        outputText.setText("");
        txtVersion.setText(quadVersion);
        LOG.info("QuadUI {} lauched with JAVA {} under OS {}", quadVersion, System.getProperty("java.runtime.version"), System.getProperty("os.name"));
        mode = "none";

        convertButton.getButtonPressListeners().add(new ButtonPressListener() {

            @Override
            public void buttonPressed(Button button) {
                ArrayList<String> validationErrors = validateInputs();
                if (!validationErrors.isEmpty()) {
                    final BoxPane pane = new BoxPane(Orientation.VERTICAL);
                    for (String error : validationErrors) {
                        pane.add(new Label(error));
                    }
                    Alert.alert(MessageType.ERROR, "Cannot Convert", pane, QuadUIWindow.this);
                    return;
                }
                modelSpecFiles = null;
                LOG.info("Starting translation job");
                try {
                    startTranslation();
                } catch(Exception ex) {
                    LOG.error(getStackTrace(ex));
                    if (ex.getClass().getSimpleName().equals("ZipException")) {
                        final BoxPane pane = new BoxPane(Orientation.VERTICAL);
                        pane.add(new Label("Please make sure using the latest ADA"));
                        pane.add(new Label("(no earlier than 0.3.6) to create zip file"));
                        Alert.alert(MessageType.ERROR, "Zip file broken", pane, QuadUIWindow.this);
                    } else {
                        Alert.alert(MessageType.ERROR, ex.toString(), QuadUIWindow.this);
                    }
                    enableConvertIndicator(false);
                }
            }
        });

        browseToConvert.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_raw");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv")
                                && (!file.getName().toLowerCase().endsWith(".zip")
                                && (!file.getName().toLowerCase().endsWith(".json")
                                && (!file.getName().toLowerCase().endsWith(".aceb")
                                && (!file.getName().toLowerCase().endsWith(".agmip")))))));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File convertFile = browse.getSelectedFile();
                            convertText.setText(convertFile.getPath());
                            if (outputText.getText().contains("")) {
                                try {
                                    outputText.setText(convertFile.getCanonicalFile().getParent());
                                    pref.put("last_input_raw", convertFile.getPath());
                                } catch (IOException ex) {
                                }
                            }
                            SetAutoDomeApplyMsg();
                        }
                    }
                });
            }
        });

        browseOutputDir.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse;
                if (outputText.getText().equals("")) {
//                    browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO);
                    String lastPath = pref.get("last_output", "");
                    if (lastPath.equals("") || !new File(lastPath).exists()) {
                        browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO);
                    } else {
                        File f = new File(lastPath);
                        browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO, f.getParent());
                    }
                } else {
                    browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO, outputText.getText());
                }
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File outputDir = browse.getSelectedFile();
                            outputText.setText(outputDir.getPath());
                            pref.put("last_output", outputDir.getPath());
                        }
                    }
                });
            }
        });

        browseLinkFile.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_link");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv"))
                                && (!file.getName().toLowerCase().endsWith(".zip"))
                                && (!file.getName().toLowerCase().endsWith(".aceb"))
                                );
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File linkFile = browse.getSelectedFile();
                            linkText.setText(linkFile.getPath());
                            pref.put("last_input_link", linkFile.getPath());
                            // Disable auto apply when link csv file is provided
                            txtAutoDomeApplyMsg.setText("");
                            autoApply = false;
                        }
                    }
                });
            }
        });

        browseFieldFile.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_field");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv"))
                                && (!file.getName().toLowerCase().endsWith(".zip"))
                                && (!file.getName().toLowerCase().endsWith(".aceb")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File fieldFile = browse.getSelectedFile();
                            fieldText.setText(fieldFile.getPath());
                            pref.put("last_input_field", fieldFile.getPath());
                        }
                    }
                });
            }
        });

        browseStrategyFile.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_strategy");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv"))
                                && (!file.getName().toLowerCase().endsWith(".zip"))
                                && (!file.getName().toLowerCase().endsWith(".aceb")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File strategyFile = browse.getSelectedFile();
                            strategyText.setText(strategyFile.getPath());
                            pref.put("last_input_strategy", strategyFile.getPath());
                        }
                    }
                });
            }
        });

        runType.getButtonGroupListeners().add(new ButtonGroupListener() {
            @Override
            public void buttonAdded(ButtonGroup group, Button prev) {}

            @Override
            public void buttonRemoved(ButtonGroup group, Button prev) {}

            @Override
            public void selectionChanged(ButtonGroup group, Button prev) {
                String current = group.getSelection().getName();
                // For DEBUG only
                if (current.equals("overlayNone")) {
                    enableLinkFile(false);
                    enableFieldOverlay(false);
                    enableStrategyOverlay(false);
                    mode = "none";
                } else if (current.equals("overlayField")) {
                    enableLinkFile(true);
                    enableFieldOverlay(true);
                    enableStrategyOverlay(false);
                    mode = "field";

                } else if (current.equals("overlaySeasonal")) {
                    enableLinkFile(true);
                    enableFieldOverlay(true);
                    enableStrategyOverlay(true);
                    mode = "strategy";
                }
            }
        });

        optionLinkage.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                linkBP.setVisible(state.equals(State.UNSELECTED));
            }
        });

        initCheckBox(modelApsim, "last_model_select_apsim");
        initCheckBox(modelDssat, "last_model_select_dssat");
        initCheckBox(modelCgnau, "last_model_select_cgnau");
        initCheckBox(modelStics, "last_model_select_stics");
        initCheckBox(modelWofost, "last_model_select_wofost");
        initCheckBox(modelJson, "last_model_select_json");
        initCheckBox(optionCompress, "last_option_select_compress");
        initCheckBox(optionOverwrite, "last_option_select_overwrite");
    }

    private void startTranslation() throws Exception {
        enableConvertIndicator(true);
        txtStatus.setText("Importing data...");
        LOG.info("Importing data...");
        if (convertText.getText().endsWith(".json")) {
            try {
                // Load the JSON representation into memory and send it down the line.
                String json = new Scanner(new File(convertText.getText()), "UTF-8").useDelimiter("\\A").next();
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
                if (mode.equals("none")) {
                    toOutput(data, null);
                } else {
                    LOG.info("Attempting to apply a new DOME");
                    applyDome(data, mode);
                }
            } catch (Exception ex) {
                LOG.error(getStackTrace(ex));
            }
        } else if (convertText.getText().endsWith(".aceb")) {
            try {
                // Load the ACE Binay file into memory and transform it to old JSON format and send it down the line.
                AceDataset ace = AceParser.parseACEB(new File(convertText.getText()));
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

                if (mode.equals("none")) {
                    toOutput(data, null);
                } else {
                    LOG.info("Attempting to apply a new DOME");
                    applyDome(data, mode);
                }
            } catch (Exception ex) {
                LOG.error(getStackTrace(ex));
            }
        } else {
            TranslateFromTask task = new TranslateFromTask(convertText.getText());
            TaskListener<HashMap> listener = new TaskListener<HashMap>() {

                @Override
                public void taskExecuted(Task<HashMap> t) {
                    HashMap data = t.getResult();
                    if (!data.containsKey("errors")) {
                        modelSpecFiles = (HashMap) data.remove("ModelSpec");
                        dumpToAceb(data);
                        if (mode.equals("none")) {
                            toOutput(data, null);
                        } else {
                            applyDome(data, mode);
                        }
                    } else {
                        Alert.alert(MessageType.ERROR, (String) data.get("errors"), QuadUIWindow.this);
                        enableConvertIndicator(false);
                    }
                }

                @Override
                public void executeFailed(Task<HashMap> arg0) {
                    Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                    LOG.error(getStackTrace(arg0.getFault()));
                    enableConvertIndicator(false);
                }
            };
            task.execute(new TaskAdapter<HashMap>(listener));
        }
    }

    protected void dumpToAceb(HashMap map) {
        dumpToAceb(map, false);
    }

    protected void dumpToAceb(HashMap map, final boolean isDome) {
        if (!isDome) {
            generateId(map);
        }
        String filePath = convertText.getText();
        final String fileName = new File(filePath).getName();
        final HashMap result = (HashMap) map.get("domeoutput");
        boolean isSkipped = false;
        boolean isSkippedForLink = false;
        if (map == null || (!isDome && filePath.toUpperCase().endsWith(".ACEB"))) {
            return;
        } else if (isDome && fieldText.getText().toUpperCase().endsWith(".ACEB") && strategyText.getText().toUpperCase().endsWith(".ACEB")) {
            isSkipped = true;
        }
        if (linkText.getText().toUpperCase().endsWith(".ACEB")) {
            isSkippedForLink = true;
        }
        if (isSkipped) {
            txtStatus.setText("Skip generating ACE Binary file for DOMEs applied for " + fileName + " ...");
            LOG.info("Skip generating ACE Binary file for DOMEs applied for {} ...", fileName);
        } else if (isDome) {
            txtStatus.setText("Generate ACE Binary file for DOMEs applied for " + fileName + " ...");
            LOG.info("Generate ACE Binary file for DOMEs applied for {} ...", fileName);
        } else {
            txtStatus.setText("Generate ACE Binary file for " + fileName + " ...");
            LOG.info("Generate ACE Binary file for {} ...", fileName);
        }
        if (isSkippedForLink) {
            txtStatus.setText("Skip generating ACE Binary file for linkage information used for " + fileName + " ...");
            LOG.info("Skip generating ACE Binary file for linkage information used for {} ...", fileName);
        }
        DumpToAceb task = new DumpToAceb(filePath, outputText.getText(), map, isDome, isSkipped, isSkippedForLink);
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
                Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                if (isDome) {
                    toOutput(result, null);
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
                HashMap expData = JSONAdapter.fromJSON(new String(exp.rebuildComponent()));
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

    private void applyDome(HashMap map, String mode) {
        txtStatus.setText("Applying DOME...");
        LOG.info("Applying DOME...");
        ApplyDomeTask task = new ApplyDomeTask(linkText.getText(), fieldText.getText(), strategyText.getText(), mode, map, autoApply);
        TaskListener<HashMap> listener = new TaskListener<HashMap>() {
            @Override
            public void taskExecuted(Task<HashMap> t) {
                HashMap data = t.getResult();
                if (!data.containsKey("errors")) {
                    //LOG.error("Domeoutput: {}", data.get("domeoutput"));
                    dumpToAceb(data, true);
//                    dumpToAceb(fieldText.getText(), (HashMap) data.get("ovlDomes"));
//                    dumpToAceb(strategyText.getText(), (HashMap) data.get("stgDomes"));
//                    dumpToAceb(linkText.getText(), (HashMap) data.get("linkDomes"));
//                    toOutput((HashMap) data.get("domeoutput"));
                } else {
                    Alert.alert(MessageType.ERROR, (String) data.get("errors"), QuadUIWindow.this);
                    enableConvertIndicator(false);
                }
            }

            @Override
            public void executeFailed(Task<HashMap> arg0) {
                Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                LOG.error(getStackTrace(arg0.getFault()));
                enableConvertIndicator(false);
            }
        };
        task.execute(new TaskAdapter<HashMap>(listener));
    }

    private void toOutput(HashMap map, HashMap<String, String> domeIdHashMap) {
        
        // ********************** DEUBG ************************
//        try {
//            AceDataset ace = AceParser.parse(toJSON(map));
//            File f = new File(outputText.getText() + "/" + mode + "_ace.txt");
//            FileWriter fw = new FileWriter(f);
//            for (AceExperiment exp : ace.getExperiments()) {
//                fw.append(exp.getValueOr("exname", "N/A"));
//                fw.append("\t");
//                fw.append(exp.getId());
//                fw.append("\r\n");
//            }
//            for (AceSoil soil : ace.getSoils()) {
//                fw.append(soil.getValueOr("soil_id", "N/A"));
//                fw.append("\t");
//                fw.append(soil.getId());
//                fw.append("\r\n");
//            }
//            for (AceWeather wth : ace.getWeathers()) {
//                fw.append(wth.getValueOr("wst_id", "N/A"));
//                fw.append("\t");
//                fw.append(wth.getId());
//                fw.append("\r\n");
//            }
//            fw.flush();
//            fw.close();
//        } catch (IOException e) {
//        }
        // ********************** DEUBG ************************
        txtStatus.setText("Generating model input files...");
        ArrayList<String> models = new ArrayList<String>();
        if (modelJson.isSelected()) {
            models.add("JSON");
        }
        if (modelApsim.isSelected()) {
            models.add("APSIM");
        }
        if (modelDssat.isSelected()) {
            models.add("DSSAT");
        }
        if (modelStics.isSelected()) {
            models.add("STICS");
        }
        if (modelWofost.isSelected()) {
            models.add("WOFOST");
        }
        if (modelCgnau.isSelected()) {
            models.add("CropGrow-NAU");
        }
        if (optionOverwrite.isSelected()) {
            LOG.info("Clean the previous output folders...");
            String outPath = outputText.getText() + File.separator;
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
            DumpToJson task = new DumpToJson(convertText.getText(), outputText.getText(), map);
            TaskListener<String> listener = new TaskListener<String>() {

                @Override
                public void taskExecuted(Task<String> t) {
                    LOG.info("Dump to JSON successfully");
                    txtStatus.setText("Completed");
                    Alert.alert(MessageType.INFO, "Translation completed", QuadUIWindow.this);
                    enableConvertIndicator(false);
                }

                @Override
                public void executeFailed(Task<String> arg0) {
                    LOG.info("Dump to JSON failed");
                    LOG.error(getStackTrace(arg0.getFault()));
                    Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                    enableConvertIndicator(false);
                }
            };
            task.execute(new TaskAdapter<String>(listener));
        } else {
            if (models.indexOf("JSON") != -1) {
                DumpToJson task = new DumpToJson(convertText.getText(), outputText.getText(), map);
                TaskListener<String> listener = new TaskListener<String>() {

                    @Override
                    public void taskExecuted(Task<String> t) {
                        LOG.info("Dump to JSON successfully");
//                        toOutput2(models, t.getResult());
                    }

                    @Override
                    public void executeFailed(Task<String> arg0) {
                        LOG.info("Dump to JSON failed");
                        LOG.error(getStackTrace(arg0.getFault()));
                        Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
//                        enableConvertIndicator(false);
                    }
                };
                task.execute(new TaskAdapter<String>(listener));
            }
            toOutput2(models, map, domeIdHashMap);
        }
    }
    
    private void toOutput2(ArrayList<String> models, HashMap map, HashMap<String, String> domeIdHashMap) {
        TranslateToTask task = new TranslateToTask(models, map, outputText.getText(), optionCompress.isSelected(), domeIdHashMap, modelSpecFiles);
        TaskListener<String> listener = new TaskListener<String>() {
            @Override
            public void executeFailed(Task<String> arg0) {
                Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                LOG.error(getStackTrace(arg0.getFault()));
                enableConvertIndicator(false);
            }

            @Override
            public void taskExecuted(Task<String> arg0) {
                txtStatus.setText("Completed");
                Alert.alert(MessageType.INFO, "Translation completed", QuadUIWindow.this);
                enableConvertIndicator(false);
                LOG.info("=== Completed translation job ===");
            }
        };
        task.execute(new TaskAdapter<String>(listener));
    }

    private static String getStackTrace(Throwable aThrowable) {
//        final Writer result = new StringWriter();
//        final PrintWriter printWriter = new PrintWriter(result);
//        aThrowable.printStackTrace(printWriter);
//        return result.toString();
        return Functions.getStackTrace(aThrowable);
    }

    private void enableLinkFile(boolean enabled) {
            lblLink.setEnabled(enabled);
            linkText.setEnabled(enabled);
            browseLinkFile.setEnabled(enabled);
    }

    private void enableFieldOverlay(boolean enabled) {
        lblField.setEnabled(enabled);
        fieldText.setEnabled(enabled);
        browseFieldFile.setEnabled(enabled);
    }

    private void enableStrategyOverlay(boolean enabled) {
        lblStrategy.setEnabled(enabled);
        strategyText.setEnabled(enabled);
        browseStrategyFile.setEnabled(enabled);
    }
    
    private void enableConvertIndicator(boolean enabled) {
        convertIndicator.setActive(enabled);
        convertButton.setEnabled(!enabled);
    }

    private void SetAutoDomeApplyMsg() {
        File convertFile = new File(convertText.getText());
        String fileName = convertFile.getName().toLowerCase();
        String msg = "";
        autoApply = false;
        if (fileName.endsWith(".zip")) {
            try {
                ZipFile zf = new ZipFile(convertFile);
                Enumeration<? extends ZipEntry> e = zf.entries();
                while (e.hasMoreElements()) {
                    ZipEntry ze = (ZipEntry) e.nextElement();
                    String zeName = ze.getName().toLowerCase();
                    if (!zeName.endsWith(".csv")) {
                        msg = "Selected DOME will be Auto applied";
                        autoApply = true;
                    } else {
                        msg = "";
                        autoApply = false;
                        break;
                    }
                }
                zf.close();
            } catch (IOException ex) {
            }

        } else if (!fileName.endsWith(".csv")) {
            msg = "Selected DOME will be Auto applied";
            autoApply = true;
        }
        txtAutoDomeApplyMsg.setText(msg);
//        if (autoApply) {
//            QuadUILinkSheet s;
//            try {
//                s = (QuadUILinkSheet) new BXMLSerializer().readObject(getClass().getResource("/link_sheet.bxml"));
//                s.open(QuadUIWindow.this);
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            } catch (SerializationException ex) {
//                ex.printStackTrace();
//            }
//        }
    }

    private FileBrowserSheet openFileBrowserSheet(String lastPathId) {
        if (convertText.getText().equals("")) {
            String lastPath = pref.get(lastPathId, "");
            File tmp = new File(lastPath);
            if (lastPath.equals("") || !tmp.exists()) {
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
            } else {
                if (!tmp.isDirectory()) {
                    lastPath = tmp.getParentFile().getPath();
                }
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, lastPath);
            }
        } else {
            try {
                String path = new File(convertText.getText()).getCanonicalFile().getParent();
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, path);
            } catch (IOException ex) {
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
            }
        }
    }
    
    private void initCheckBox(Checkbox cb, final String lastSelectId) {
        cb.setSelected(pref.getBoolean(lastSelectId, false));
        cb.getButtonStateListeners().add(new ButtonStateListener() {

            @Override
            public void stateChanged(Button button, State state) {
                pref.putBoolean(lastSelectId, button.isSelected());
            }
        });
    }
}
