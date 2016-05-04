package org.agmip.ui.quadui;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.prefs.Preferences;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.agmip.common.Functions;
import org.agmip.dome.BatchEngine;
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

    private static final Logger LOG = LoggerFactory.getLogger(QuadUIWindow.class);
    private ActivityIndicator convertIndicator = null;
    private PushButton convertButton = null;
    private PushButton browseExpToConvert = null;
    private PushButton browseWthToConvert = null;
    private PushButton browseCulToConvert = null;
    private PushButton browseSoilToConvert = null;
    private PushButton browseOutputDir = null;
    private PushButton browseLinkFile = null;
    private PushButton browseBatchFile = null;
    private PushButton browseFieldFile = null;
    private PushButton browseStrategyFile = null;
    private ButtonGroup runType = null;
    private Checkbox convertExpCB = null;
    private Checkbox convertWthCB = null;
    private Checkbox convertCulCB = null;
    private Checkbox convertSoilCB = null;
    private Checkbox modelApsim = null;
    private Checkbox modelDssat = null;
    private Checkbox modelSarrah33 = null;
    private Checkbox modelStics = null;
    private Checkbox modelWofost = null;
    private Checkbox modelCgnau = null;
    private Checkbox modelJson = null;
    private Checkbox optionCompress = null;
    private Checkbox optionOverwrite = null;
    private Checkbox optionLinkage = null;
    private Checkbox optionBatch = null;
    private Checkbox optionAcebOnly = null;
    private Label txtStatus = null;
    private Label txtAutoDomeApplyMsg = null;
    private Label txtVersion = null;
    private Label lblLink = null;
    private Label lblBatch = null;
    private Label lblField = null;
    private Label lblStrategy = null;
    private TextInput outputText = null;
    private TextInput convertExpText = null;
    private TextInput convertWthText = null;
    private TextInput convertCulText = null;
    private TextInput convertSoilText = null;
    private TextInput linkText = null;
    private TextInput batchText = null;
    private TextInput fieldText = null;
    private TextInput strategyText = null;
    private ArrayList<Checkbox> checkboxGroup = new ArrayList<Checkbox>();
    private ArrayList<String> errors = new ArrayList<String>();
    private Properties versionProperties = new Properties();
    private String quadVersion = "";
    private Preferences pref = null;
    private String mode = "";
    private boolean autoApply = false;
    private boolean acebOnly = false;
    private boolean acebOnlyRet = true;
//    private boolean isMultiInput = false;
    private boolean isExpActived = true;
    private boolean isWthActived = true;
    private boolean isCulActived = true;
    private boolean isSoilActived = false;
    private HashMap modelSpecFiles;
    HashMap<String, Object> batch;
    private BatchEngine batEngine;
    private String outputDir = "";
    private boolean isBatchApplied;

    public QuadUIWindow() {
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

        Action.getNamedActions().put("fileQuit", new Action() {
            @Override
            public void perform(Component src) {
                DesktopApplicationContext.exit();
            }
        });
    }

    private ArrayList<String> validateInputs() {
        ArrayList<String> errs = new ArrayList<String>();
        if (!acebOnly) {
            boolean anyModelChecked = false;
            for (Checkbox cbox : checkboxGroup) {
                if (cbox.isSelected()) {
                    anyModelChecked = true;
                }
            }
            if (!anyModelChecked) {
                errs.add("You need to select an output format");
            }
        }
        if (!isExpActived && !isWthActived && !isSoilActived) {
            errs.add("Experiment, weather or soil data is required");
        } else {
            if (isExpActived) {
                validateInputFile(errs, convertExpText.getText(), "experiment", new String[]{});
            }
            if (isWthActived) {
                validateInputFile(errs, convertWthText.getText(), "weather", new String[]{".csv", ".wth", ".met", ".agmip", ".aceb", ".json"});
            }
            if (isSoilActived) {
                validateInputFile(errs, convertSoilText.getText(), "soil", new String[]{".csv", ".sol", ".aceb", ".json"});
            }
        }
        if (isCulActived) {
            validateInputFile(errs, convertCulText.getText(), "cultivar", new String[]{"_specific/"});
        }
        
        File dir = new File(outputText.getText());
        if (!dir.exists() || !dir.isDirectory()) {
            errs.add("You need to select an output directory");
        }
        return errs;
    }
    
    private void validateInputFile(ArrayList<String> errs, String filePath, String dataType, String[] allowedExts) {
        File convertFile = new File(filePath);
        if (!convertFile.exists()) {
            errs.add("You need to include a " + dataType + " data to convert");
        } else if (convertFile.getName().toLowerCase().endsWith(".zip") && allowedExts.length > 0) {
            try {
                ZipFile zf = new ZipFile(convertFile);
                Enumeration<? extends ZipEntry> e = zf.entries();
                boolean isCulExist = false;
                while (e.hasMoreElements()) {
                    ZipEntry ze = (ZipEntry) e.nextElement();
                    String zeName = ze.getName().toLowerCase();
                    boolean isAllowExt = false;
                    if (dataType.equals("cultivar")) {
                        if (ze.isDirectory() && zeName.endsWith(allowedExts[0])) {
                            isCulExist = true;
                        }
                    } else {
                        if (ze.isDirectory()) {
                            isAllowExt = true;
                        } else {
                            for (String ext : allowedExts) {
                                if (zeName.endsWith(ext)) {
                                    isAllowExt = true;
                                    break;
                                }
                            }
                        }
                        if (!isAllowExt) {
                            errs.add("Your " + dataType +" data contains non-" + dataType + " files");
                            break;
                        }
                    }
                }
                if (dataType.equals("cultivar") && !isCulExist) {
                    errs.add("Your " + dataType +" data don't contains " + dataType + " folder");
                }
                zf.close();
            } catch (IOException ex) {
            }
        }
    }

    @Override
    public void initialize(Map<String, Object> ns, URL location, Resources res) {
        convertIndicator    = (ActivityIndicator) ns.get("convertIndicator");
        convertButton       = (PushButton) ns.get("convertButton");
        browseExpToConvert     = (PushButton) ns.get("browseConvertExpButton");
        browseWthToConvert     = (PushButton) ns.get("browseConvertWthButton");
        browseCulToConvert     = (PushButton) ns.get("browseConvertCulButton");
        browseSoilToConvert     = (PushButton) ns.get("browseConvertSoilButton");
        browseOutputDir     = (PushButton) ns.get("browseOutputButton");
        browseLinkFile      = (PushButton) ns.get("browseLinkButton");
        browseBatchFile     = (PushButton) ns.get("browseBatchButton");
        browseFieldFile     = (PushButton) ns.get("browseFieldButton");
        browseStrategyFile  = (PushButton) ns.get("browseStrategyButton");
        runType             = (ButtonGroup) ns.get("runTypeButtons");
        txtStatus           = (Label) ns.get("txtStatus");
        txtAutoDomeApplyMsg = (Label) ns.get("txtAutoDomeApplyMsg");
        txtVersion          = (Label) ns.get("txtVersion");
        lblLink             = (Label) ns.get("linkLabel");
        lblBatch             = (Label) ns.get("batchLabel");
        lblField            = (Label) ns.get("fieldLabel");
        lblStrategy         = (Label) ns.get("strategyLabel");
        convertExpText         = (TextInput) ns.get("convertExpText");
        convertWthText      = (TextInput) ns.get("convertWthText");
        convertCulText      = (TextInput) ns.get("convertCulText");
        convertSoilText      = (TextInput) ns.get("convertSoilText");
        outputText          = (TextInput) ns.get("outputText");
        linkText            = (TextInput) ns.get("linkText");
        batchText           = (TextInput) ns.get("batchText");
        fieldText           = (TextInput) ns.get("fieldText");
        strategyText        = (TextInput) ns.get("strategyText");
        convertExpCB        = (Checkbox) ns.get("convertExpCB");
        convertWthCB        = (Checkbox) ns.get("convertWthCB");
        convertCulCB        = (Checkbox) ns.get("convertCulCB");
        convertSoilCB        = (Checkbox) ns.get("convertSoilCB");
        modelApsim          = (Checkbox) ns.get("model-apsim");
        modelDssat          = (Checkbox) ns.get("model-dssat");
        modelSarrah33       = (Checkbox) ns.get("model-sarrah33");
        modelStics          = (Checkbox) ns.get("model-stics");
        modelWofost         = (Checkbox) ns.get("model-wofost");
        modelCgnau          = (Checkbox) ns.get("model-cgnau");
        modelJson           = (Checkbox) ns.get("model-json");
        optionCompress      = (Checkbox) ns.get("option-compress");
        optionOverwrite     = (Checkbox) ns.get("option-overwrite");
        optionLinkage       = (Checkbox) ns.get("option-linkage");
        optionBatch         = (Checkbox) ns.get("option-batch");
        optionAcebOnly      = (Checkbox) ns.get("option-acebonly");

        checkboxGroup.add(modelApsim);
        checkboxGroup.add(modelDssat);
        checkboxGroup.add(modelSarrah33);
        checkboxGroup.add(modelStics);
        checkboxGroup.add(modelWofost);
        checkboxGroup.add(modelCgnau);
        checkboxGroup.add(modelJson);

        outputText.setText("");
        txtVersion.setText(quadVersion);
        LOG.info("QuadUI {} lauched with JAVA {} under OS {}", quadVersion, System.getProperty("java.runtime.version"), System.getProperty("os.name"));
        try {
            pref = Preferences.userNodeForPackage(getClass());
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }
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

                LOG.info("Starting translation job");
                try {
                    batch = null;
                    batEngine = null;
                    if (batchText.isEnabled() && !batchText.getText().equals("")) {
                        prepareBatchRun();
                    } else {
                        startTranslation();
                    }

                } catch (Exception ex) {
                    LOG.error(getStackTrace(ex));
                    if (ex.getClass().getSimpleName().equals("ZipException")) {
                        final BoxPane pane = new BoxPane(Orientation.VERTICAL);
                        pane.add(new Label("Please make sure using the latest ADA"));
                        pane.add(new Label("(no earlier than 0.3.6) to create zip file"));
                        Alert.alert(MessageType.ERROR, "Zip file broken", pane, QuadUIWindow.this);
                    } else {
                        Alert.alert(MessageType.ERROR, ex.toString(), QuadUIWindow.this);
                    }
//                    }
                    quitCurRun(false, isBatchApplied);
                }
            }
        });

        browseExpToConvert.getButtonPressListeners().add(new ButtonPressListener() {
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
                            convertExpText.setText(convertFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_raw", convertFile.getPath());
                            }
                            if (outputText.getText().contains("")) {
                                try {
                                    outputText.setText(convertFile.getCanonicalFile().getParent());
                                } catch (IOException ex) {
                                }
                            }
                            SetAutoDomeApplyMsg();
                        }
                    }
                });
            }
        });

        browseWthToConvert.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_raw_wth");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && !file.getName().toLowerCase().endsWith(".csv")
                                && !file.getName().toLowerCase().endsWith(".zip")
                                && !file.getName().toLowerCase().endsWith(".json")
                                && !file.getName().toLowerCase().endsWith(".aceb")
                                && !file.getName().toLowerCase().endsWith(".wth")
                                && !file.getName().toLowerCase().endsWith(".agmip"));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File convertWthFile = browse.getSelectedFile();
                            convertWthText.setText(convertWthFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_raw_wth", convertWthFile.getPath());
                            }
                        }
                    }
                });
            }
        });

        browseCulToConvert.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_raw_cul");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && !file.getName().toLowerCase().endsWith(".zip"));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File convertCulFile = browse.getSelectedFile();
                            convertCulText.setText(convertCulFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_raw_cul", convertCulFile.getPath());
                            }
                        }
                    }
                });
            }
        });

        browseSoilToConvert.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_raw_soil");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && !file.getName().toLowerCase().endsWith(".csv")
                                && !file.getName().toLowerCase().endsWith(".zip")
                                && !file.getName().toLowerCase().endsWith(".json")
                                && !file.getName().toLowerCase().endsWith(".aceb")
                                && !file.getName().toLowerCase().endsWith(".sol")
                                && !file.getName().toLowerCase().endsWith(".agmip"));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File convertSoilFile = browse.getSelectedFile();
                            convertSoilText.setText(convertSoilFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_raw_soil", convertSoilFile.getPath());
                            }
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
                    String lastPath = "";
                    if (pref != null) {
                        lastPath = pref.get("last_output", "");
                    }
                    if (lastPath.equals("") || !new File(lastPath).exists()) {
                        browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO);
                    } else {
                        File f = new File(lastPath);
                        browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO, f.getParent());
                    }
                } else {
                    if (!new File(outputText.getText()).exists()) {
                        browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO);
                    } else {
                        browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO, outputText.getText());
                    }
                }
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File dir = browse.getSelectedFile();
                            outputText.setText(dir.getPath());
                            if (pref != null) {
                                pref.put("last_output", dir.getPath());
                            }
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
                                //                                && (!file.getName().toLowerCase().endsWith(".zip"))
                                && (!file.getName().toLowerCase().endsWith(".alnk")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File linkFile = browse.getSelectedFile();
                            linkText.setText(linkFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_link", linkFile.getPath());
                            }
                            // Disable auto apply when link csv file is provided
                            txtAutoDomeApplyMsg.setText("");
                            autoApply = false;
                        }
                    }
                });
            }
        });

        browseBatchFile.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = openFileBrowserSheet("last_input_batch");
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File batchFile = browse.getSelectedFile();
                            batchText.setText(batchFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_batch", batchFile.getPath());
                            }
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
                                && (!file.getName().toLowerCase().endsWith(".dome")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File fieldFile = browse.getSelectedFile();
                            fieldText.setText(fieldFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_field", fieldFile.getPath());
                            }
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
                                && (!file.getName().toLowerCase().endsWith(".dome")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File strategyFile = browse.getSelectedFile();
                            strategyText.setText(strategyFile.getPath());
                            if (pref != null) {
                                pref.put("last_input_strategy", strategyFile.getPath());
                            }
                        }
                    }
                });
            }
        });

        runType.getButtonGroupListeners().add(new ButtonGroupListener() {
            @Override
            public void buttonAdded(ButtonGroup group, Button prev) {
            }

            @Override
            public void buttonRemoved(ButtonGroup group, Button prev) {
            }

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
                    if (optionLinkage.isSelected()) {
                        enableLinkFile(true);
                    }
                    enableFieldOverlay(true);
                    enableStrategyOverlay(false);
                    mode = "field";

                } else if (current.equals("overlaySeasonal")) {
                    if (optionLinkage.isSelected()) {
                        enableLinkFile(true);
                    }
                    enableFieldOverlay(true);
                    enableStrategyOverlay(true);
                    mode = "strategy";
                }
            }
        });

        convertExpCB.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                isExpActived = state.equals(State.UNSELECTED);
                convertExpText.setEnabled(isExpActived);
                browseExpToConvert.setEnabled(isExpActived);
            }
        });

        convertWthCB.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                isWthActived = state.equals(State.UNSELECTED);
                convertWthText.setEnabled(isWthActived);
                browseWthToConvert.setEnabled(isWthActived);
            }
        });

        convertCulCB.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                isCulActived = state.equals(State.UNSELECTED);
                convertCulText.setEnabled(isCulActived);
                browseCulToConvert.setEnabled(isCulActived);
            }
        });

        convertSoilCB.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                isSoilActived = state.equals(State.UNSELECTED);
                convertSoilText.setEnabled(isSoilActived);
                browseSoilToConvert.setEnabled(isSoilActived);
            }
        });

        optionLinkage.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                if (state.equals(State.UNSELECTED)) {
                    if (!mode.equals("none")) {
                        enableLinkFile(true);
                    }
                } else {
                    enableLinkFile(false);
                    linkText.setText("");
                    SetAutoDomeApplyMsg();
                }
            }
        });

        optionBatch.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                if (state.equals(State.UNSELECTED)) {
                    enableBatchFile(true);
                } else {
                    enableBatchFile(false);
                }
            }
        });

        optionAcebOnly.getButtonStateListeners().add(new ButtonStateListener() {
            @Override
            public void stateChanged(Button button, State state) {
                acebOnly = state.equals(State.SELECTED);
                modelApsim.setEnabled(acebOnly);
                modelDssat.setEnabled(acebOnly);
                modelSarrah33.setEnabled(acebOnly);
                modelStics.setEnabled(acebOnly);
                modelWofost.setEnabled(acebOnly);
                modelCgnau.setEnabled(acebOnly);
                modelJson.setEnabled(acebOnly);
                acebOnly = !acebOnly;
            }
        });

        initCheckBox(modelApsim, "last_model_select_apsim");
        initCheckBox(modelDssat, "last_model_select_dssat");
        initCheckBox(modelSarrah33, "last_model_select_sarrah33");
        initCheckBox(modelCgnau, "last_model_select_cgnau");
        initCheckBox(modelStics, "last_model_select_stics");
        initCheckBox(modelWofost, "last_model_select_wofost");
        initCheckBox(modelJson, "last_model_select_json");
        initCheckBox(optionCompress, "last_option_select_compress");
        initCheckBox(optionOverwrite, "last_option_select_overwrite");
    }

    private void prepareBatchRun() {
        enableConvertIndicator(true);
        LOG.info("Loading batch file...");
        this.batch = QuadUtil.loadBatchFile(batchText.getText());
        this.batEngine = new BatchEngine(this.batch);
//        QuadUtil.cleanBatchDir(outputText.getText(), optionOverwrite.isSelected());
        if (batEngine.hasNext()) {
            startTranslation();
        }
    }

    private void startTranslation() {
        isBatchApplied = false;
        enableConvertIndicator(true);
        outputDir = QuadUtil.getOutputDir(outputText.getText(), optionOverwrite.isSelected(), batEngine);
        txtStatus.setText(getCurBatchInfo() + "Importing data...");
        LOG.info(getCurBatchInfo() + "Importing data...");
        TranslateFromTask task;
        ArrayList<String> inputFiles = new ArrayList();
        if (isExpActived) {
            inputFiles.add(convertExpText.getText());
        }
        if (isWthActived) {
            inputFiles.add(convertWthText.getText());
        }
        if (isCulActived) {
            inputFiles.add(convertCulText.getText());
        }
        if (isSoilActived) {
            inputFiles.add(convertSoilText.getText());
        }
        try {
            task = new TranslateFromTask(inputFiles.toArray(new String[0]));
            TaskListener<HashMap> listener = new TaskListener<HashMap>() {

                @Override
                public void taskExecuted(Task<HashMap> t) {
                    HashMap data = t.getResult();
                    if (!data.containsKey("errors")) {
                        modelSpecFiles = (HashMap) data.remove("ModelSpec");

                        // Dump input data into aceb format
                        boolean isDomeApplied = true;
                        if (isExpActived && (isWthActived || isSoilActived)) {
                            isDomeApplied = false;
                        } else if (isWthActived && isSoilActived) {
                            isDomeApplied = false;
                        } else {
                            if (isExpActived) {
                                isDomeApplied = QuadUtil.isDomeApplied(convertExpText.getText().toLowerCase(), data);
                            } else {
                                if (isWthActived) {
                                    isDomeApplied = QuadUtil.isDomeApplied(convertWthText.getText().toLowerCase(), data);
                                }
                                if (isDomeApplied && isSoilActived) {
                                    isDomeApplied = QuadUtil.isDomeApplied(convertSoilText.getText().toLowerCase(), data);
                                }
                            }
                        }

                        if (!isDomeApplied) {
                            dumpToAceb(data);
                        }

                        if (batEngine != null) {
                            applyBatch(data);
                        } else {
                            if (mode.equals("none")) {
                                if (!acebOnly) {
                                    toOutput(data, null);
                                }
                            } else {
                                applyDome(data, mode, new ArrayList());
                            }
                        }

                    } else {
                        Alert.alert(MessageType.ERROR, (String) data.get("errors"), QuadUIWindow.this);
                        quitCurRun(false, isBatchApplied);
                    }
                }

                @Override
                public void executeFailed(Task<HashMap> arg0) {
                    Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                    LOG.error(getCurBatchInfo() + getStackTrace(arg0.getFault()));
                    quitCurRun(false, isBatchApplied);
                }
            };
            task.execute(new TaskAdapter<HashMap>(listener));
        } catch (Exception ex) {
            LOG.error(getCurBatchInfo() + getStackTrace(ex));
            if (ex.getClass().getSimpleName().equals("ZipException")) {
                final BoxPane pane = new BoxPane(Orientation.VERTICAL);
                pane.add(new Label("Please make sure using the latest ADA"));
                pane.add(new Label("(no earlier than 0.3.6) to create zip file"));
                Alert.alert(MessageType.ERROR, "Zip file broken", pane, QuadUIWindow.this);
            } else {
                Alert.alert(MessageType.ERROR, ex.toString(), QuadUIWindow.this);
            }
            quitCurRun(false, isBatchApplied);
        }
    }

    private void applyBatch(HashMap data) {

        // Apply batch DOME
        final String batchGID = batEngine.getCurGroupId();
        LOG.info(getCurBatchInfo() + "Applying batch [" + batEngine.getBatchName() + "]...");

        ApplyBatchTask task = new ApplyBatchTask(data, batEngine);
        TaskListener<HashMap> listener = new TaskListener<HashMap>() {

            @Override
            public void taskExecuted(Task<HashMap> task) {
                isBatchApplied = true;
                HashMap data = task.getResult();
                if (mode.equals("none")) {
                    if (!acebOnly) {
                        toOutput(data, null);
                    }
                } else {
                    applyDome(data, mode, batEngine.currentModifiedVarList());
                }
            }

            @Override
            public void executeFailed(Task<HashMap> task) {
                Alert.alert(MessageType.ERROR, task.getFault().toString(), QuadUIWindow.this);
                isBatchApplied = !batchGID.equals(batEngine.getCurGroupId());
                LOG.error(getCurBatchInfo() + getStackTrace(task.getFault()));
                quitCurRun(false, isBatchApplied);
            }
        };
        task.execute(new TaskAdapter<HashMap>(listener));
    }

    protected void dumpToAceb(HashMap map) {
        dumpToAceb(map, false);
    }

    protected void dumpToAceb(HashMap map, final boolean isDome) {
        if (!isDome) {
            QuadUtil.generateId(map);
        }
        String filePath;
        if (!isDome) {
            filePath = convertExpText.getText();
        } else if (mode.equals("strategy")) {
            filePath = strategyText.getText();
        } else if (mode.equals("field")) {
            filePath = fieldText.getText();
        } else {
            filePath = convertExpText.getText();
        }
        String filePathL;
        if (linkText.getText().trim().equals("")) {
            filePathL = convertExpText.getText();
        } else {
            filePathL = linkText.getText();
        }
        final String fileName = new File(convertExpText.getText()).getName();
        final HashMap result = (HashMap) map.get("domeoutput");
        boolean isSkipped = false;
        boolean isSkippedForLink = false;
        if (!isDome && convertExpText.getText().toUpperCase().endsWith(".ACEB")) {
            return;
        } else if (isDome && fieldText.getText().toUpperCase().endsWith(".DOME") && strategyText.getText().toUpperCase().endsWith(".DOME")) {
            isSkipped = true;
        }
        if (linkText.getText().toUpperCase().endsWith(".ALNK")) {
            isSkippedForLink = true;
        }
        if (isSkipped) {
            txtStatus.setText(getCurBatchInfo() + "Skip generating ACE Binary file for DOMEs applied for " + fileName + " ...");
            LOG.info(getCurBatchInfo() + "Skip generating ACE Binary file for DOMEs applied for {} ...", fileName);
        } else if (isDome) {
            txtStatus.setText(getCurBatchInfo() + "Generate DOME file for DOMEs applied for " + fileName + " ...");
            LOG.info(getCurBatchInfo() + "Generate DOME file for DOMEs applied for {} ...", fileName);
        } else {
            txtStatus.setText(getCurBatchInfo() + "Generate ACE Binary file for " + fileName + " ...");
            LOG.info(getCurBatchInfo() + "Generate ACE Binary file for {} ...", fileName);
        }
        if (isSkippedForLink) {
            txtStatus.setText(getCurBatchInfo() + "Skip generating ALNK file for " + fileName + " ...");
            LOG.info(getCurBatchInfo() + "Skip generating ALNK file for {} ...", fileName);
        }
        DumpToAceb task = new DumpToAceb(filePath, filePathL, outputText.getText(), map, isDome, isSkipped, isSkippedForLink);
        TaskListener<HashMap<String, String>> listener = new TaskListener<HashMap<String, String>>() {
            @Override
            public void taskExecuted(Task<HashMap<String, String>> t) {
                if (isDome) {
                    LOG.info(getCurBatchInfo() + "Dump to ACE Binary for DOMEs applied for {} successfully", fileName);
                } else {
                    LOG.info(getCurBatchInfo() + "Dump to ACE Binary for {} successfully", fileName);
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
                    LOG.info(getCurBatchInfo() + "Dump to ACE Binary for DOMEs applied for {} failed", fileName);
                } else {
                    LOG.info(getCurBatchInfo() + "Dump to ACE Binary for {} failed", fileName);
                }
                LOG.error(getCurBatchInfo() + getStackTrace(arg0.getFault()));
                Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                if (acebOnly) {
                    acebOnlyRet = false;
                }
                if (acebOnly) {
                    toOutput(result, null);
                } else if (isDome) {
                    QuadUtil.reviseData(result);
                    toOutput(result, null);
                }
            }
        };
        task.execute(new TaskAdapter<HashMap<String, String>>(listener));
    }

    private void applyDome(HashMap map, String mode, ArrayList<String> skipVarList) {
        txtStatus.setText(getCurBatchInfo() + "Applying DOME...");
        LOG.info(getCurBatchInfo() + "Applying DOME...");
        ApplyDomeTask task = new ApplyDomeTask(linkText.getText(), fieldText.getText(), strategyText.getText(), mode, map, skipVarList, autoApply, acebOnly);
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
//                    dumpToAceb(fieldText.getText(), (HashMap) data.get("ovlDomes"));
//                    dumpToAceb(strategyText.getText(), (HashMap) data.get("stgDomes"));
//                    dumpToAceb(linkText.getText(), (HashMap) data.get("linkDomes"));
//                    toOutput((HashMap) data.get("domeoutput"));
                } else {
                    Alert.alert(MessageType.ERROR, (String) data.get("errors"), QuadUIWindow.this);
                    quitCurRun(false, isBatchApplied);
                }
            }

            @Override
            public void executeFailed(Task<HashMap> arg0) {
                Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                LOG.error(getCurBatchInfo() + getStackTrace(arg0.getFault()));
                quitCurRun(false, isBatchApplied);
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
        if (acebOnly) {
            if (acebOnlyRet) {
                txtStatus.setText(getCurBatchInfo() + "Completed");
                if (batEngine == null || !batEngine.hasNext()) {
                    Alert.alert(MessageType.INFO, "Translation completed", QuadUIWindow.this);
                    LOG.info("=== Completed translation job ===");
                }
                
                enableConvertIndicator(false);
                return;
            } else {
                txtStatus.setText(getCurBatchInfo() + "Failed");
                Alert.alert(MessageType.ERROR, "Translation failed", QuadUIWindow.this);
                quitCurRun(false, isBatchApplied);
                return;
            }
        }
        txtStatus.setText(getCurBatchInfo() + "Generating model input files...");
        domeIdHashMap = QuadUtil.saveDomeHashedIds(map, domeIdHashMap);
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
        if (modelSarrah33.isSelected()) {
            models.add("SarraHV33");
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
            LOG.info(getCurBatchInfo() + "Clean the previous output folders...");
            String outPath = outputText.getText() + File.separator;
            for (String model : models) {
                if (model.equalsIgnoreCase("JSON")) {
                    continue;
                }
                File dir = new File(outPath + model);
                if (!Functions.clearDirectory(dir)) {
                    LOG.warn(getCurBatchInfo() + "Failed to clean {} folder since it is being used by other process", model);
                }
            }
        }
        LOG.info(getCurBatchInfo() + "Generating model input files...");

        if (models.size() == 1 && models.get(0).equals("JSON")) {
            DumpToJson task = new DumpToJson(convertExpText.getText(), outputDir, map);
            TaskListener<String> listener = new TaskListener<String>() {

                @Override
                public void taskExecuted(Task<String> t) {
                    LOG.info(getCurBatchInfo() + "Dump to JSON successfully");
                    txtStatus.setText(getCurBatchInfo() + "Completed");
                    if (batEngine == null || !batEngine.hasNext()) {
                        Alert.alert(MessageType.INFO, "Translation completed", QuadUIWindow.this);
                        LOG.info("=== Completed translation job ===");
                    }
                    enableConvertIndicator(false);
                }

                @Override
                public void executeFailed(Task<String> arg0) {
                    LOG.info(getCurBatchInfo() + "Dump to JSON failed");
                    LOG.error(getCurBatchInfo() + getStackTrace(arg0.getFault()));
                    Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                    quitCurRun(false, isBatchApplied);
                }
            };
            task.execute(new TaskAdapter<String>(listener));
        } else {
            if (models.indexOf("JSON") != -1) {
                DumpToJson task = new DumpToJson(convertExpText.getText(), outputDir, map);
                TaskListener<String> listener = new TaskListener<String>() {

                    @Override
                    public void taskExecuted(Task<String> t) {
                        LOG.info(getCurBatchInfo() + "Dump to JSON successfully");
//                        toOutput2(models, t.getResult());
                    }

                    @Override
                    public void executeFailed(Task<String> arg0) {
                        LOG.info(getCurBatchInfo() + "Dump to JSON failed");
                        LOG.error(getCurBatchInfo() + getStackTrace(arg0.getFault()));
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
        TranslateToTask task = new TranslateToTask(models, map, outputDir, optionCompress.isSelected(), domeIdHashMap, modelSpecFiles);
        TaskListener<String> listener = new TaskListener<String>() {
            @Override
            public void executeFailed(Task<String> arg0) {
                Alert.alert(MessageType.ERROR, arg0.getFault().toString(), QuadUIWindow.this);
                LOG.error(getCurBatchInfo() + getStackTrace(arg0.getFault()));
                enableConvertIndicator(false);
            }

            @Override
            public void taskExecuted(Task<String> arg0) {
                txtStatus.setText(getCurBatchInfo() + "Completed");
                if (batEngine == null || !batEngine.hasNext()) {
                    Alert.alert(MessageType.INFO, "Translation completed", QuadUIWindow.this);
                    LOG.info("=== Completed translation job ===");
                }
                quitCurRun(false, isBatchApplied);
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
        lblLink.repaint();
    }

    private void enableBatchFile(boolean enabled) {
        lblBatch.setEnabled(enabled);
        batchText.setEnabled(enabled);
        browseBatchFile.setEnabled(enabled);
        lblBatch.repaint();
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
        if (!enabled && batEngine != null && batEngine.hasNext()) {
            LOG.info("=== Batch " + batEngine.getNextGroupId() + " finished ===");
            startTranslation();
        } else {
            convertIndicator.setActive(enabled);
            convertButton.setEnabled(!enabled);
        }
    }

    private void quitCurRun(boolean enabled, boolean isBatchApplied) {
        txtStatus.setText(txtStatus.getText() + "failed!");
        if (batEngine != null && !isBatchApplied) {
            batEngine.getNextBatchRun();
        }
        if (!enabled && batEngine != null && batEngine.hasNext()) {
            LOG.info("=== Batch " + QuadUtil.getCurGroupId(batEngine, isBatchApplied) + " failed ===");
            startTranslation();
        } else {
            convertIndicator.setActive(enabled);
            convertButton.setEnabled(!enabled);
        }
    }

    private void SetAutoDomeApplyMsg() {
        File convertFile = new File(convertExpText.getText());
        String fileName = convertFile.getName().toLowerCase();
        String msg = "";
        autoApply = false;
        if (!linkText.getText().equals("")) {
            msg = "";
            autoApply = false;
        } else if (fileName.endsWith(".zip")) {
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
        if (!convertExpText.getText().equals("")) {
            try {
                String path = new File(convertExpText.getText()).getCanonicalFile().getParent();
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, path);
            } catch (IOException ex) {
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
            }
        } else if (!convertWthText.getText().equals("")) {
            try {
                String path = new File(convertWthText.getText()).getCanonicalFile().getParent();
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, path);
            } catch (IOException ex) {
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
            }
        } else if (!convertCulText.getText().equals("")) {
            try {
                String path = new File(convertCulText.getText()).getCanonicalFile().getParent();
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, path);
            } catch (IOException ex) {
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
            }
        } else if (!convertSoilText.getText().equals("")) {
            try {
                String path = new File(convertSoilText.getText()).getCanonicalFile().getParent();
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, path);
            } catch (IOException ex) {
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
            }
        } else {
            String lastPath = "";
            if (pref != null) {
                lastPath = pref.get(lastPathId, "");
            }
            File tmp = new File(lastPath);
            if (lastPath.equals("") || !tmp.exists()) {
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
            } else {
                if (!tmp.isDirectory()) {
                    lastPath = tmp.getParentFile().getPath();
                }
                return new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, lastPath);
            }
        }
    }

    private void initCheckBox(Checkbox cb, final String lastSelectId) {
        boolean lastChoice = false;
        if (pref != null) {
            lastChoice = pref.getBoolean(lastSelectId, false);
        }
        cb.setSelected(lastChoice);
        cb.getButtonStateListeners().add(new ButtonStateListener() {

            @Override
            public void stateChanged(Button button, State state) {
                if (pref != null) {
                    pref.putBoolean(lastSelectId, button.isSelected());
                }
            }
        });
    }

    private String getCurBatchInfo() {
        return QuadUtil.getCurBatchInfo(batEngine, isBatchApplied);
    }
}
