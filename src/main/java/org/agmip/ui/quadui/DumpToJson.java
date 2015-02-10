package org.agmip.ui.quadui;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;

import static org.agmip.util.JSONAdapter.toJSON;
import org.agmip.util.MapUtil;

public class DumpToJson extends Task<String> {

    private final HashMap data;
    private final String fileName, directoryName;

    public DumpToJson(String file, String dirName, HashMap data) {
        this.fileName = file;
        this.directoryName = dirName;
        this.data = data;
    }

    @Override
    public String execute() throws TaskExecutionException {
        FileWriter fw;
        BufferedWriter bw;
        String base = getBaseFileName(fileName);
        String outputJson = directoryName + "/" + getJsonName(data, base) + ".json";
        File file = new File(outputJson);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }

            fw = new FileWriter(file.getAbsoluteFile());
            bw = new BufferedWriter(fw);
            bw.write(toJSON(data));
            bw.close();
            fw.close();
        } catch (IOException ex) {
            throw new TaskExecutionException(ex);
        }
        return null;
    }

    private String getJsonName(HashMap data, String base) {
        String ret;
        String adp = "";
        String dome = "";
        if (base.matches(".+-[Aa]\\d+$")) {
            adp = base.substring(base.lastIndexOf("-"));
            ret = base.substring(0, base.lastIndexOf("-"));
        } else {
            ret = base;
        }

        String climId = "";
        ArrayList<HashMap> exps = MapUtil.getObjectOr(data, "experiments", new ArrayList());
        for (HashMap exp : exps) {
            climId = MapUtil.getValueOr(exp, "clim_id", "");
            if (!climId.equals("")) {
                break;
            }
        }
        for (HashMap exp : exps) {
            String domeFlag = MapUtil.getValueOr(exp, "dome_applied", "");
            if ("Y".equals(domeFlag)) {
                String seasonalFlag = MapUtil.getValueOr(exp, "seasonal_dome_applied", "");
                if ("Y".equals(seasonalFlag)) {
                    dome = "-seasonal_strategy";
                    break;
                }
                String rotationalFlag = MapUtil.getValueOr(exp, "rotational_dome_applied", "");
                if ("Y".equals(seasonalFlag)) {
                    dome = "-rotational_strategy";
                    break;
                }
                String fieldFlag = MapUtil.getValueOr(exp, "field_dome_applied", "");
                if ("Y".equals(seasonalFlag)) {
                    dome = "-field_overlay";
                }
            }
            break;
        }
        if (climId.equals("")) {
            ArrayList<HashMap> wths = MapUtil.getObjectOr(data, "weathers", new ArrayList());
            for (HashMap wth : wths) {
                climId = MapUtil.getValueOr(wth, "clim_id", "");
                if (!climId.equals("")) {
                    break;
                }
            }
        }

        if (climId.equals("")) {
            return base + dome;
        } else {
            return ret + "-" + climId + adp + dome;
        }
    }

    private String getBaseFileName(String f) {
        File file = new File(f);
        String[] base = file.getName().split("\\.(?=[^\\.]+$)");
        return base[0];
    }
}
