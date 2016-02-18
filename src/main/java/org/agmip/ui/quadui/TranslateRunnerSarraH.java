package org.agmip.ui.quadui;

import java.nio.file.Path;
import org.agmip.ace.AceDataset;
import org.agmip.translators.sarrah.AceDatasetToSarraH;


public class TranslateRunnerSarraH extends TranslateRunner2 {
    
    public TranslateRunnerSarraH(AceDataset aceData, String outputDirectory, String model, boolean compress) {
        super(aceData, outputDirectory, model, compress);
    }

    @Override
    protected void runTranslation(AceDataset aceData, Path outputDirectory) {
        AceDatasetToSarraH.write(aceData, outputDirectory);
    }
}
