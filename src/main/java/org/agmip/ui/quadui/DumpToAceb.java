package org.agmip.ui.quadui;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.rits.cloning.Cloner;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import org.agmip.ace.io.AceGenerator;
import org.agmip.dome.DomeUtil;
import org.agmip.translators.csv.AlnkOutput;
import static org.agmip.util.JSONAdapter.toJSON;
import org.agmip.util.MapUtil;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpToAceb extends Task<HashMap<String, String>> {

    private final HashMap data;
    private final String fileName, directoryName, linkFileName;
    private final boolean isDome;
    private final boolean isSkipped;
    private final boolean isSkippedForLink;
    private static final HashFunction hf = Hashing.sha256();
    private HashMap domeIdHashMap = new HashMap();
    private HashMap domeHashData = null;
    private static final Logger LOG = LoggerFactory.getLogger(DumpToAceb.class);

    public DumpToAceb(String file, String fileL, String dirName, HashMap data, boolean isDome, boolean isSkipped, boolean isSkippedForLink) {
        this.fileName = file;
        this.linkFileName = fileL;
        this.directoryName = dirName;
        Cloner cloner = new Cloner();
        this.data = cloner.deepClone(data);
        this.isDome = isDome;
        this.isSkipped = isSkipped;
        this.isSkippedForLink = isSkippedForLink;
    }

    @Override
    public HashMap<String, String> execute() throws TaskExecutionException {
        String base = getBaseFileName(fileName);
        String baseL = getBaseFileName(linkFileName);
        String outputAceb;
        String ext;
        if (isDome) {
            outputAceb = directoryName + "/" + getDomeName(base, baseL) + ".dome";
            ext = ".dome";
        } else {
            outputAceb = directoryName + "/" + getAcebName(data, base, baseL) + ".aceb";
            ext = ".aceb";
        }
        File file = new File(outputAceb);
        int count = 1;
        while (file.isFile() && !file.canWrite()) {
            file = new File(file.getPath().replaceAll(".+_\\d*" + ext, "_" + count + ext));
            count++;
        }
        try {
//            if (!file.exists()) {
//                file.createNewFile();
//            }
            if (!isDome) {
                if (!isSkipped) {
                    AceGenerator.generateACEB(file, toJSON(data));
                }
            } else {
                domeHashData = new HashMap();
                domeIdHashMap = new HashMap();
                buildDomeHash("ovlDomes");
                buildDomeHash("stgDomes");
                buildDomeHash("batDomes");
                if (!isSkipped) {
                    AceGenerator.generateACEB(file, toJSON(domeHashData));
                }
                if (!isSkippedForLink) {
                    file = new File(directoryName + "/" + baseL + ".alnk");
                    AlnkOutput writer = new AlnkOutput();
                    writer.writeFile(file.getPath(), MapUtil.getObjectOr(data, "domeoutput", new HashMap()));
                }
                domeHashData = null;
                return domeIdHashMap;
            }
        } catch (IOException ex) {
            throw new TaskExecutionException(ex);
        }
        return null;
    }
    
    private void buildDomeHash(String domeType) throws IOException {
        HashMap<String, HashMap> domes = MapUtil.getObjectOr(data, domeType, new HashMap());
        for (String domeId : domes.keySet()) {
            String hash = generateHCId(domes.get(domeId)).toString();
            domeHashData.put(hash, domes.get(domeId));
            domeIdHashMap.put(DomeUtil.generateDomeName(domes.get(domeId)), hash);
        }
    }
    
    private HashCode generateHCId(HashMap data) throws IOException {
        LinkedHashMap sortedData = sortMap(data);
        String jsonStr = toJSON(sortedData);
        LOG.info("DOME {} : {}", DomeUtil.generateDomeName(data), jsonStr);
        return hf.newHasher().putBytes(jsonStr.getBytes("UTF-8")).hash();
    }
    
    private String getDomeName(String base, String baseL) {
        if (!base.matches(".+-[Aa]\\d+$") && baseL.matches(".+-[Aa]\\d+$")) {
            return base + baseL.substring(baseL.lastIndexOf("-"));
        } else {
            return base;
        }
    }

    private String getAcebName(HashMap data, String base, String baseL) {
        String ret;
        String adp = "";
        if (base.matches(".+-[Aa]\\d+$")) {
            adp = base.substring(base.lastIndexOf("-"));
            ret = base.substring(0, base.lastIndexOf("-"));
        } else if (baseL.matches(".+-[Aa]\\d+$")) {
            adp = baseL.substring(baseL.lastIndexOf("-"));
            ret = base;
        } else {
            ret = base;
        }

        String climId = "";
        boolean found = false;
        ArrayList<HashMap> exps = MapUtil.getObjectOr(data, "experiments", new ArrayList());
        for (HashMap exp : exps) {
            String tmp = MapUtil.getValueOr(exp, "clim_id", "");
            if (!tmp.equals("")) {
                if (found && tmp.equals(climId)) {
                    climId = "";
                    break;
                } else {
                    climId = tmp;
                    found = true;
                }
                
            }
        }
        if (!found && climId.equals("")) {
            ArrayList<HashMap> wths = MapUtil.getObjectOr(data, "weathers", new ArrayList());
            for (HashMap wth : wths) {
                String tmp = MapUtil.getValueOr(wth, "clim_id", "");
                if (found && tmp.equals(climId)) {
                    climId = "";
                    break;
                } else {
                    climId = tmp;
                    found = true;
                }
            }
        }

        if (climId.equals("")) {
            return base;
        } else {
            return ret + "-" + climId + adp;
        }
    }

    private String getBaseFileName(String f) {
        File file = new File(f);
        String[] base = file.getName().split("\\.(?=[^\\.]+$)");
        return base[0];
    }
    
    private LinkedHashMap sortMap(HashMap<String, Object> data) {
        List<String> sortedKeys = new ArrayList(data.keySet());
        Collections.sort(sortedKeys);
        LinkedHashMap sortedData = new LinkedHashMap();
        for (String key : sortedKeys) {
            Object value = data.get(key);
            if (value instanceof HashMap) {
                sortedData.put(key, sortMap((HashMap) value));
            } else if (value instanceof ArrayList) {
                sortMap((ArrayList) value);
            } else {
                sortedData.put(key, value);
            }
        }
        return sortedData;
    }
    
    private void sortMap(ArrayList arr) {
        for (int i = 0; i < arr.size(); i++) {
            if (arr.get(i) instanceof HashMap) {
                arr.set(i, sortMap((HashMap) arr.get(i)));
            }
        }
    }
}
