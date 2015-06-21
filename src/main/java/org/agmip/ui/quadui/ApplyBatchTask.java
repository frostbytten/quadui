package org.agmip.ui.quadui;

import java.util.HashMap;
import org.agmip.dome.BatchEngine;
import org.apache.pivot.util.concurrent.Task;
import org.apache.pivot.util.concurrent.TaskExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Meng Zhang
 */
public class ApplyBatchTask extends Task<HashMap> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ApplyBatchTask.class);
    private final BatchEngine batEngine;
    private final HashMap source;
//    private final int thrPoolSize;
    
    public ApplyBatchTask(HashMap m, BatchEngine batEngine) {
        this.source = m;
        this.batEngine = batEngine;
//        thrPoolSize = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public HashMap execute() throws TaskExecutionException {
        batEngine.applyNext(source);
        return source;
    }
    
}
