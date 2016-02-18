/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.agmip.ui.quadui;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import org.agmip.ace.AceDataset;
import org.agmip.common.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mike
 */
public abstract class TranslateRunner2 implements Runnable {
    
    private final AceDataset aceData;
    private final Path outputDirectory;
    private final String model;
    private final boolean compress;
    private final static Logger LOG = LoggerFactory.getLogger(TranslateRunner2.class);

    public TranslateRunner2(AceDataset aceData, String outputDirectory, String model, boolean compress) {
        this.aceData = aceData;
        this.outputDirectory = new File(outputDirectory).toPath();
        this.model = model;
        this.compress = compress;
    }
    
    protected abstract void runTranslation(AceDataset aceData, Path outputDirectory);

    @Override
    public void run() {
        LOG.debug("Starting new thread!");
        long timer = System.currentTimeMillis();
        try {
//            translatorClass.getMethod("write", AceDataset.class, Path.class).invoke(null, aceData, outputDirectory);
            runTranslation(this.aceData, this.outputDirectory);
            if (compress) {
                QuadUtil.compressOutput(outputDirectory.toString(), model);
            }
            timer = System.currentTimeMillis() - timer;
            LOG.info("{} Translator Finished in {}s.", model, timer/1000.0);
        } catch (IOException ex) {
            LOG.error("{} translator got an error.", model);
            LOG.error(Functions.getStackTrace(ex));
        }
    }
}
