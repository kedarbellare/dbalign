package cc.refectorie.user.kedarb.dbalign;

import cc.refectorie.user.kedarb.tools.opts.Opt;

import java.util.Random;

/**
 * @author kedarb
 * @since 5/10/11
 */
public class SegCorefOptions {
    @Opt(gloss = "Count threshold for word features")
    public int wordCountThreshold = 1;

    @Opt(gloss = "Count threshold for features")
    public int featureCountThreshold = 1;

    @Opt(gloss = "Number of threads")
    public int numThreads = 1;

    @Opt(gloss = "Maximum number of records")
    public int maxRecords = Integer.MAX_VALUE;

    @Opt(gloss = "Maximum number of texts")
    public int maxTexts = Integer.MAX_VALUE;

    @Opt(gloss = "Weight of text mentions during learning for segmentation only")
    public double segTextLearnWeight = 0.0;

    @Opt(gloss = "Weight of text mentions during learning for coref+segmentation")
    public double corefSegTextLearnWeight = 0.0;

    @Opt(gloss = "Step size multiplier")
    public double stepSizeMultiplier = 1.0;

    @Opt(gloss = "Step size offset")
    public double stepSizeOffset = 2.0;

    @Opt(gloss = "Step size reduction power")
    public double stepSizeReductionPower = 0.5;

    @Opt(gloss = "Similarity weight")
    public double simWeight = 10.0;

    @Opt(gloss = "Inverse variance for weights")
    public double invVariance = 1.0;

    @Opt(gloss = "Random number generator for online learning")
    public Random onlineRandom = new Random(1);

    @Opt(gloss = "Batch size during mini-batch online learning")
    public int miniBatchSize = 10;

    @Opt(gloss = "Rexa labeled citations for evaluation")
    public String rexaTestFile = "data/hlabeled_dbalign.txt";

    @Opt(gloss = "Rexa test proportion")
    public double rexaTestProportion = 0.2;

    @Opt(gloss = "Output results every this number of iterations")
    public int evalIterationCount = 1;

    @Opt
    public String wordFile = "rexa.word.vocab";

    @Opt
    public String wordFeatureFile = "rexa.word_features.vocab";

    @Opt
    public String featureFile = "rexa.features.vocab";

    @Opt
    public boolean segTextLearnConstrained = false;

    @Opt
    public boolean segTextInferConstrained = false;

    @Opt
    public int pingIterationCount = 100;

    // Iterations:
    // 1) gen
    // 2) disc
    // 3) kl_disc
    // 4) coref_gen
    // 5) coref_disc
    // 6) coref_kl_disc
    @Opt
    public int segGenLearnIter = 5;

    @Opt
    public int segDiscLearnIter = 100;

    @Opt
    public int segKLDiscLearnIter = 100;

    @Opt
    public int corefSegGenLearnIter = 10;

    @Opt
    public int corefSegDiscLearnIter = 20;

    @Opt
    public int corefSegKLDiscLearnIter = 0;
}
