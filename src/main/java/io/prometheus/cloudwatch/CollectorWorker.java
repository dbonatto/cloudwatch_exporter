package io.prometheus.cloudwatch;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.*;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class CollectorWorker implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(CollectorWorker.class.getName());

    private List<Collector.MetricFamilySamples> mfs;
    private long start;
    private CloudWatchCollector.MetricRule rule;
    private List<String> brokenDynamoMetrics;
    private AmazonCloudWatch client;
    private Counter cloudwatchRequests;

    public CollectorWorker(List<Collector.MetricFamilySamples> mfs, long start, CloudWatchCollector.MetricRule rule, List<String> brokenDynamoMetrics, AmazonCloudWatch client, Counter cloudwatchRequests) {
        this.mfs = mfs;
        this.start = start;
        this.rule = rule;
        this.brokenDynamoMetrics = brokenDynamoMetrics;
        this.client = client;
        this.cloudwatchRequests = cloudwatchRequests;
    }

    @Override
    public void run() {
        try {
            doRun();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while running the collector thread.", e);

        }
    }

    private void doRun() {
        Date startDate = new Date(start - 1000 * rule.delaySeconds);
        Date endDate = new Date(start - 1000 * (rule.delaySeconds + rule.rangeSeconds));
        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
        request.setNamespace(rule.awsNamespace);
        request.setMetricName(rule.awsMetricName);
        request.setStatistics(rule.awsStatistics);
        request.setExtendedStatistics(rule.awsExtendedStatistics);
        request.setEndTime(startDate);
        request.setStartTime(endDate);
        request.setPeriod(rule.periodSeconds);

        String baseName = safeName(rule.awsNamespace.toLowerCase() + "_" + toSnakeCase(rule.awsMetricName));
        String jobName = safeName(rule.awsNamespace.toLowerCase());
        List<Collector.MetricFamilySamples.Sample> sumSamples = new ArrayList<Collector.MetricFamilySamples.Sample>();
        List<Collector.MetricFamilySamples.Sample> sampleCountSamples = new ArrayList<Collector.MetricFamilySamples.Sample>();
        List<Collector.MetricFamilySamples.Sample> minimumSamples = new ArrayList<Collector.MetricFamilySamples.Sample>();
        List<Collector.MetricFamilySamples.Sample> maximumSamples = new ArrayList<Collector.MetricFamilySamples.Sample>();
        List<Collector.MetricFamilySamples.Sample> averageSamples = new ArrayList<Collector.MetricFamilySamples.Sample>();
        HashMap<String, ArrayList<Collector.MetricFamilySamples.Sample>> extendedSamples = new HashMap<String, ArrayList<Collector.MetricFamilySamples.Sample>>();

        String unit = null;

        if (rule.awsNamespace.equals("AWS/DynamoDB")
                && rule.awsDimensions.contains("GlobalSecondaryIndexName")
                && brokenDynamoMetrics.contains(rule.awsMetricName)) {
            baseName += "_index";
        }

        for (List<Dimension> dimensions : getDimensions(rule)) {
            request.setDimensions(dimensions);

            GetMetricStatisticsResult result = client.getMetricStatistics(request);
            cloudwatchRequests.inc();
            Datapoint dp = getNewestDatapoint(result.getDatapoints());
            if (dp == null) {
                continue;
            }
            unit = dp.getUnit();

            List<String> labelNames = new ArrayList<String>();
            List<String> labelValues = new ArrayList<String>();
            labelNames.add("job");
            labelValues.add(jobName);
            labelNames.add("instance");
            labelValues.add("");
            for (Dimension d : dimensions) {
                labelNames.add(safeName(toSnakeCase(d.getName())));
                labelValues.add(d.getValue());
            }

            if (dp.getSum() != null) {
                sumSamples.add(new Collector.MetricFamilySamples.Sample(
                        baseName + "_sum", labelNames, labelValues, dp.getSum()));
            }
            if (dp.getSampleCount() != null) {
                sampleCountSamples.add(new Collector.MetricFamilySamples.Sample(
                        baseName + "_sample_count", labelNames, labelValues, dp.getSampleCount()));
            }
            if (dp.getMinimum() != null) {
                minimumSamples.add(new Collector.MetricFamilySamples.Sample(
                        baseName + "_minimum", labelNames, labelValues, dp.getMinimum()));
            }
            if (dp.getMaximum() != null) {
                maximumSamples.add(new Collector.MetricFamilySamples.Sample(
                        baseName + "_maximum", labelNames, labelValues, dp.getMaximum()));
            }
            if (dp.getAverage() != null) {
                averageSamples.add(new Collector.MetricFamilySamples.Sample(
                        baseName + "_average", labelNames, labelValues, dp.getAverage()));
            }
            if (dp.getExtendedStatistics() != null) {
                for (Map.Entry<String, Double> entry : dp.getExtendedStatistics().entrySet()) {
                    ArrayList<Collector.MetricFamilySamples.Sample> samples = extendedSamples.get(entry.getKey());
                    if (samples == null) {
                        samples = new ArrayList<Collector.MetricFamilySamples.Sample>();
                        extendedSamples.put(entry.getKey(), samples);
                    }
                    samples.add(new Collector.MetricFamilySamples.Sample(
                            baseName + "_" + safeName(toSnakeCase(entry.getKey())), labelNames, labelValues, entry.getValue()));
                }
            }
        }

        if (!sumSamples.isEmpty()) {
            mfs.add(new Collector.MetricFamilySamples(baseName + "_sum", Collector.Type.GAUGE, help(rule, unit, "Sum"), sumSamples));
        }
        if (!sampleCountSamples.isEmpty()) {
            mfs.add(new Collector.MetricFamilySamples(baseName + "_sample_count", Collector.Type.GAUGE, help(rule, unit, "SampleCount"), sampleCountSamples));
        }
        if (!minimumSamples.isEmpty()) {
            mfs.add(new Collector.MetricFamilySamples(baseName + "_minimum", Collector.Type.GAUGE, help(rule, unit, "Minimum"), minimumSamples));
        }
        if (!maximumSamples.isEmpty()) {
            mfs.add(new Collector.MetricFamilySamples(baseName + "_maximum", Collector.Type.GAUGE, help(rule, unit, "Maximum"), maximumSamples));
        }
        if (!averageSamples.isEmpty()) {
            mfs.add(new Collector.MetricFamilySamples(baseName + "_average", Collector.Type.GAUGE, help(rule, unit, "Average"), averageSamples));
        }
        for (Map.Entry<String, ArrayList<Collector.MetricFamilySamples.Sample>> entry : extendedSamples.entrySet()) {
            mfs.add(new Collector.MetricFamilySamples(baseName + "_" + safeName(toSnakeCase(entry.getKey())), Collector.Type.GAUGE, help(rule, unit, entry.getKey()), entry.getValue()));
        }
    }

    private String safeName(String s) {
        // Change invalid chars to underscore, and merge underscores.
        return s.replaceAll("[^a-zA-Z0-9:_]", "_").replaceAll("__+", "_");
    }

    private String help(CloudWatchCollector.MetricRule rule, String unit, String statistic) {
        if (rule.help != null) {
            return rule.help;
        }
        return "CloudWatch metric " + rule.awsNamespace + " " + rule.awsMetricName
                + " Dimensions: " + rule.awsDimensions + " Statistic: " + statistic
                + " Unit: " + unit;
    }

    private String toSnakeCase(String str) {
        return str.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
    }

    private List<List<Dimension>> getDimensions(CloudWatchCollector.MetricRule rule) {
        List<List<Dimension>> dimensions = new ArrayList<List<Dimension>>();
        if (rule.awsDimensions == null) {
            dimensions.add(new ArrayList<Dimension>());
            return dimensions;
        }

        ListMetricsRequest request = new ListMetricsRequest();
        request.setNamespace(rule.awsNamespace);
        request.setMetricName(rule.awsMetricName);
        List<DimensionFilter> dimensionFilters = new ArrayList<DimensionFilter>();
        for (String dimension : rule.awsDimensions) {
            dimensionFilters.add(new DimensionFilter().withName(dimension));
        }
        request.setDimensions(dimensionFilters);

        String nextToken = null;
        do {
            request.setNextToken(nextToken);
            ListMetricsResult result = client.listMetrics(request);
            cloudwatchRequests.inc();
            for (Metric metric : result.getMetrics()) {
                if (metric.getDimensions().size() != dimensionFilters.size()) {
                    // AWS returns all the metrics with dimensions beyond the ones we ask for,
                    // so filter them out.
                    continue;
                }
                if (useMetric(rule, metric)) {
                    dimensions.add(metric.getDimensions());
                }
            }
            nextToken = result.getNextToken();

        } while (nextToken != null);



        return dimensions;
    }

    /**
     * Check if a metric should be used according to `aws_dimension_select` or `aws_dimension_select_regex`
     */
    private boolean useMetric(CloudWatchCollector.MetricRule rule, Metric metric) {
        if (rule.awsDimensionSelect == null && rule.awsDimensionSelectRegex == null) {
            return true;
        }
        if (rule.awsDimensionSelect != null && metricsIsInAwsDimensionSelect(rule, metric)) {
            return true;
        }
        if (rule.awsDimensionSelectRegex != null && metricIsInAwsDimensionSelectRegex(rule, metric)) {
            return true;
        }
        return false;
    }


    /**
     * Check if a metric is matched in `aws_dimension_select`
     */
    private boolean metricsIsInAwsDimensionSelect(CloudWatchCollector.MetricRule rule, Metric metric) {
        Set<String> dimensionSelectKeys = rule.awsDimensionSelect.keySet();
        for (Dimension dimension : metric.getDimensions()) {
            String dimensionName = dimension.getName();
            String dimensionValue = dimension.getValue();
            if (dimensionSelectKeys.contains(dimensionName)) {
                List<String> allowedDimensionValues = rule.awsDimensionSelect.get(dimensionName);
                if (!allowedDimensionValues.contains(dimensionValue)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Check if a metric is matched in `aws_dimension_select_regex`
     */
    private boolean metricIsInAwsDimensionSelectRegex(CloudWatchCollector.MetricRule rule, Metric metric) {
        Set<String> dimensionSelectRegexKeys = rule.awsDimensionSelectRegex.keySet();
        for (Dimension dimension : metric.getDimensions()) {
            String dimensionName = dimension.getName();
            String dimensionValue = dimension.getValue();
            if (dimensionSelectRegexKeys.contains(dimensionName)) {
                List<String> allowedDimensionValues = rule.awsDimensionSelectRegex.get(dimensionName);
                if (!regexListMatch(allowedDimensionValues, dimensionValue)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Check if any regex string in a list matches a given input value
     */
    protected static boolean regexListMatch(List<String> regexList, String input) {
        for (String regex: regexList) {
            if (Pattern.matches(regex, input)) {
                return true;
            }
        }
        return false;
    }

    private Datapoint getNewestDatapoint(List<Datapoint> datapoints) {
        Datapoint newest = null;
        for (Datapoint d: datapoints) {
            if (newest == null || newest.getTimestamp().before(d.getTimestamp())) {
                newest = d;
            }
        }
        return newest;
    }
}
