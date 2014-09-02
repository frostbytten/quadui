package org.agmip.ui.quadui;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.rits.cloning.Cloner;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.agmip.ace.io.AceGenerator;
import static org.agmip.util.JSONAdapter.toJSON;
import org.agmip.util.MapUtil;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;

public class DumpToAceb extends Task<HashMap<String, String>> {

    private HashMap data;
    private String fileName, directoryName;
    private boolean isDome;
    private boolean isSkipped;
    private boolean isSkippedForLink;
    private static final HashFunction hf = Hashing.sha256();
    private HashMap domeIdHashMap = new HashMap();
    private HashMap domeHashData = null;

    public DumpToAceb(String file, String dirName, HashMap data, boolean isDome, boolean isSkipped, boolean isSkippedForLink) {
        this.fileName = file;
        this.directoryName = dirName;
        Cloner cloner = new Cloner();
        this.data = cloner.deepClone(data);
        this.isDome = isDome;
        this.isSkipped = isSkipped;
        this.isSkippedForLink = isSkippedForLink;
    }

    @Override
    public HashMap<String, String> execute() throws TaskExecutionException {
        File file = new File(fileName);
        String[] base = file.getName().split("\\.(?=[^\\.]+$)");
        String outputAceb;
        String ext;
        if (isDome) {
            if (data.containsKey("stgDomes")) {
                outputAceb = directoryName + "/" + base[0] + "_All_DOMEs.dome";
            } else {
                outputAceb = directoryName + "/" + base[0] + "_OverlayOnly_DOMEs.dome";
            }
            ext = ".dome";
        } else {
            outputAceb = directoryName + "/" + base[0] + ".aceb";
            ext = ".aceb";
        }
        file = new File(outputAceb);
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
                if (!isSkipped) {
                    AceGenerator.generateACEB(file, toJSON(domeHashData));
                }
                if (!isSkippedForLink) {
                    file = new File(directoryName + "/" + base[0] + "_Linkage.alnk");
                    HashMap<String, HashMap> linkInfo = MapUtil.getObjectOr(data, "linkDomes", new HashMap());
                    String hash = generateHCId(linkInfo).toString();
                    HashMap hashData = new HashMap();
                    hashData.put(hash, linkInfo);
                    AceGenerator.generateACEB(file, toJSON(hashData));
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
            domeIdHashMap.put(domeId, hash);
        }
    }
    
    private HashCode generateHCId(HashMap data) throws IOException {
        return hf.newHasher().putBytes(toJSON(data).getBytes("UTF-8")).hash();
    }
}
