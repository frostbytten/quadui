package org.agmip.ui.quadui;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import static org.agmip.util.JSONAdapter.*;
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
import org.apache.pivot.wtk.ButtonGroup;
import org.apache.pivot.wtk.ButtonGroupListener;
import org.apache.pivot.wtk.ButtonPressListener;
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
    private Checkbox modelJson = null;
    private Checkbox optionCompress = null;
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
    private ArrayList<Checkbox> checkboxGroup = new ArrayList<Checkbox>();
    private ArrayList<String> errors = new ArrayList<String>();
    private Properties versionProperties = new Properties();
    private String quadVersion = "";
    private String mode = "";
    private boolean autoApply = false;

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
        convertText         = (TextInput) ns.get("convertText");
        outputText          = (TextInput) ns.get("outputText");
        linkText           = (TextInput) ns.get("linkText");
        fieldText           = (TextInput) ns.get("fieldText");
        strategyText        = (TextInput) ns.get("strategyText");
        modelApsim          = (Checkbox) ns.get("model-apsim");
        modelDssat          = (Checkbox) ns.get("model-dssat");
        modelStics          = (Checkbox) ns.get("model-stics");
        modelJson           = (Checkbox) ns.get("model-json");
        optionCompress      = (Checkbox) ns.get("option-compress");

        checkboxGroup.add(modelApsim);
        checkboxGroup.add(modelDssat);
        checkboxGroup.add(modelStics);
        checkboxGroup.add(modelJson);

        outputText.setText("");
        txtVersion.setText(quadVersion);
        LOG.info("QuadUI {} lauched", quadVersion);
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
                    startTranslation();
                } catch(Exception ex) {
                    LOG.error(getStackTrace(ex));

                }
            }
        });

        browseToConvert.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse;
                if (outputText.getText().equals("")) {
                    browse = new FileBrowserSheet(FileBrowserSheet.Mode.OPEN);
                } else {
                    browse = new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, outputText.getText());
                }
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv")
                                && (!file.getName().toLowerCase().endsWith(".zip")
                                && (!file.getName().toLowerCase().endsWith(".json")
                                && (!file.getName().toLowerCase().endsWith(".agmip"))))));
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
                    browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO);
                } else {
                    browse = new FileBrowserSheet(FileBrowserSheet.Mode.SAVE_TO, outputText.getText());
                }
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File outputDir = browse.getSelectedFile();
                            outputText.setText(outputDir.getPath());
                        }
                    }
                });
            }
        });

//        browseLinkFile.getButtonPressListeners().add(new ButtonPressListener() {
//            @Override
//            public void buttonPressed(Button button) {
//                final FileBrowserSheet browse = new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, outputText.getText());
//                browse.setDisabledFileFilter(new Filter<File>() {
//
//                    @Override
//                    public boolean include(File file) {
//                        return (file.isFile()
//                                && (!file.getName().toLowerCase().endsWith(".csv"))
//                                && (!file.getName().toLowerCase().endsWith(".zip")));
//                    }
//                });
//                browse.open(QuadUIWindow.this, new SheetCloseListener() {
//                    @Override
//                    public void sheetClosed(Sheet sheet) {
//                        if (sheet.getResult()) {
//                            File linkFile = browse.getSelectedFile();
//                            linkText.setText(linkFile.getPath());
//                        }
//                    }
//                });
//            }
//        });

        browseFieldFile.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, outputText.getText());
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv"))
                                && (!file.getName().toLowerCase().endsWith(".zip")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File fieldFile = browse.getSelectedFile();
                            fieldText.setText(fieldFile.getPath());
                        }
                    }
                });
            }
        });

        browseStrategyFile.getButtonPressListeners().add(new ButtonPressListener() {
            @Override
            public void buttonPressed(Button button) {
                final FileBrowserSheet browse = new FileBrowserSheet(FileBrowserSheet.Mode.OPEN, outputText.getText());
                browse.setDisabledFileFilter(new Filter<File>() {

                    @Override
                    public boolean include(File file) {
                        return (file.isFile()
                                && (!file.getName().toLowerCase().endsWith(".csv"))
                                && (!file.getName().toLowerCase().endsWith(".zip")));
                    }
                });
                browse.open(QuadUIWindow.this, new SheetCloseListener() {
                    @Override
                    public void sheetClosed(Sheet sheet) {
                        if (sheet.getResult()) {
                            File strategyFile = browse.getSelectedFile();
                            strategyText.setText(strategyFile.getPath());
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

                if (mode.equals("none")) {
                    toOutput(data);
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
                        if (mode.equals("none")) {
                            toOutput(data);
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
                    Alert.alert(MessageType.ERROR, arg0.getFault().getMessage(), QuadUIWindow.this);
                    LOG.error(getStackTrace(arg0.getFault()));
                    enableConvertIndicator(false);
                }
            };
            task.execute(new TaskAdapter<HashMap>(listener));
        }
    }

    private void applyDome(HashMap map, String mode) {
        txtStatus.setText("Applying DOME...");
        LOG.info("Applying DOME...");
//        ApplyDomeTask task = new ApplyDomeTask(linkText.getText(), fieldText.getText(), strategyText.getText(), mode, map);
        ApplyDomeTask task = new ApplyDomeTask(fieldText.getText(), strategyText.getText(), mode, map, autoApply);
        TaskListener<HashMap> listener = new TaskListener<HashMap>() {
            @Override
            public void taskExecuted(Task<HashMap> t) {
                HashMap data = t.getResult();
                if (!data.containsKey("errors")) {
                    //LOG.error("Domeoutput: {}", data.get("domeoutput"));
                    toOutput((HashMap) data.get("domeoutput"));
                } else {
                    Alert.alert(MessageType.ERROR, (String) data.get("errors"), QuadUIWindow.this);
                    enableConvertIndicator(false);
                }
            }

            @Override
            public void executeFailed(Task<HashMap> arg0) {
                Alert.alert(MessageType.ERROR, arg0.getFault().getMessage(), QuadUIWindow.this);
                LOG.error(getStackTrace(arg0.getFault()));
                enableConvertIndicator(false);
            }
        };
        task.execute(new TaskAdapter<HashMap>(listener));
    }

    private void toOutput(HashMap map) {
        txtStatus.setText("Generating model input files...");
        LOG.info("Generating model input files...");
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

        if (models.size() == 1 && models.get(0).equals("JSON")) {
            DumpToJson task = new DumpToJson(convertText.getText(), outputText.getText(), map);
            TaskListener<String> listener = new TaskListener<String>() {

                @Override
                public void taskExecuted(Task<String> t) {
                    txtStatus.setText("Completed");
                    Alert.alert(MessageType.INFO, "Translation completed", QuadUIWindow.this);
                    enableConvertIndicator(false);
                }

                @Override
                public void executeFailed(Task<String> arg0) {
                    Alert.alert(MessageType.ERROR, arg0.getFault().getMessage(), QuadUIWindow.this);
                    LOG.error(getStackTrace(arg0.getFault()));
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
                    }

                    @Override
                    public void executeFailed(Task<String> arg0) {
                        Alert.alert(MessageType.ERROR, arg0.getFault().getMessage(), QuadUIWindow.this);
                        LOG.error(getStackTrace(arg0.getFault()));
                        enableConvertIndicator(false);
                    }
                };
                task.execute(new TaskAdapter<String>(listener));
            }
            TranslateToTask task = new TranslateToTask(models, map, outputText.getText(), optionCompress.isSelected());
            TaskListener<String> listener = new TaskListener<String>() {

                @Override
                public void executeFailed(Task<String> arg0) {
                    Alert.alert(MessageType.ERROR, arg0.getFault().getMessage(), QuadUIWindow.this);
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
    }

    private static String getStackTrace(Throwable aThrowable) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        aThrowable.printStackTrace(printWriter);
        return result.toString();
    }

    private void enableLinkFile(boolean enabled) {
//            lblLink.setEnabled(enabled);
//            linkText.setEnabled(enabled);
//            browseLinkFile.setEnabled(enabled);
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
                        break;
                    }
                }
                zf.close();
            } catch (IOException ex) {
            }

        }
        txtAutoDomeApplyMsg.setText(msg);
    }
}
