package org.agmip.ui.quadui;

import java.io.IOException;
import java.io.File;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.agmip.core.types.TranslatorOutput;

import org.agmip.ace.AceDataset;
import org.agmip.common.Functions;


public class TranslateRunner implements Runnable {
    private TranslatorOutput translator;
    private org.agmip.ace.translators.io.TranslatorOutput newTranslator;
    private HashMap data;
    private AceDataset aceData;
    private final String outputDirectory;
    private final String model;
    private final boolean compress;
    private static Logger LOG = LoggerFactory.getLogger(TranslateRunner.class);
    private final boolean isNewTranslator;

    public TranslateRunner(TranslatorOutput translator, HashMap data, String outputDirectory, String model, boolean compress) {
        this.translator = translator;
        this.data = data;
        this.outputDirectory = outputDirectory;
        this.model = model;
        this.compress = compress;
        this.isNewTranslator = false;
    }

    public TranslateRunner(org.agmip.ace.translators.io.TranslatorOutput translator, AceDataset aceData, String outputDirectory, String model, boolean compress) {
        this.newTranslator = translator;
        this.aceData = aceData;
        this.outputDirectory = outputDirectory;
        this.model = model;
        this.compress = compress;
        this.isNewTranslator = true;
    }


    @Override
    public void run() {
        LOG.debug("Starting new thread!");
        long timer = System.currentTimeMillis();
        try {
            if (isNewTranslator) {
                newTranslator.write(new File(outputDirectory), aceData);
            } else {
                translator.writeFile(outputDirectory, data);
            }
            if (compress) {
                QuadUtil.compressOutput(outputDirectory, model);
            }
            timer = System.currentTimeMillis() - timer;
            LOG.info("{} Translator Finished in {}s.", model, timer/1000.0);
        } catch (IOException e) {
            LOG.error("{} translator got an error.", model);
            LOG.error(Functions.getStackTrace(e));
        }
    }
}
