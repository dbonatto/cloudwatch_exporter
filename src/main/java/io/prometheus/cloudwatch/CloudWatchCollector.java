package io.prometheus.cloudwatch;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.Metric;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class CloudWatchCollector extends Collector {
    private static final Logger LOGGER = Logger.getLogger(CloudWatchCollector.class.getName());

    private AmazonCloudWatchClient client;
    private Region region;

    static class MetricRule {
        String awsNamespace;
        String awsMetricName;
        int periodSeconds;
        int rangeSeconds;
        int delaySeconds;
        List<String> awsStatistics;
        List<String> awsExtendedStatistics;
        List<String> awsDimensions;
        Map<String,List<String>> awsDimensionSelect;
        Map<String,List<String>> awsDimensionSelectRegex;
        String help;
    }

    private static final Counter cloudwatchRequests = Counter.build()
            .name("cloudwatch_requests_total").help("API requests made to CloudWatch").register();

    private static final List<String> brokenDynamoMetrics = Arrays.asList(
            "ConsumedReadCapacityUnits", "ConsumedWriteCapacityUnits",
            "ProvisionedReadCapacityUnits", "ProvisionedWriteCapacityUnits",
            "ReadThrottleEvents", "WriteThrottleEvents");

    private ArrayList<MetricRule> rules = new ArrayList<MetricRule>();

    public CloudWatchCollector(Reader in) throws IOException {
        this((Map<String, Object>)new Yaml().load(in),null);
    }
    public CloudWatchCollector(String yamlConfig) {
        this((Map<String, Object>)new Yaml().load(yamlConfig),null);
    }

    /* For unittests. */
    protected CloudWatchCollector(String jsonConfig, AmazonCloudWatchClient client) {
        this((Map<String, Object>)new Yaml().load(jsonConfig), client);
    }

    private CloudWatchCollector(Map<String, Object> config, AmazonCloudWatchClient client) {
        if(config == null) {  // Yaml config empty, set config to empty map.
            config = new HashMap<String, Object>();
        }
        if (!config.containsKey("region")) {
            throw new IllegalArgumentException("Must provide region");
        }
        region = RegionUtils.getRegion((String) config.get("region"));

        int defaultPeriod = 60;
        if (config.containsKey("period_seconds")) {
            defaultPeriod = ((Number)config.get("period_seconds")).intValue();
        }
        int defaultRange = 600;
        if (config.containsKey("range_seconds")) {
            defaultRange = ((Number)config.get("range_seconds")).intValue();
        }
        int defaultDelay = 600;
        if (config.containsKey("delay_seconds")) {
            defaultDelay = ((Number)config.get("delay_seconds")).intValue();
        }

        ClientConfiguration cc = new ClientConfiguration();
        String proxy = System.getenv("http_proxy");
        if(proxy != null && !proxy.isEmpty()) {
            int port = Integer.parseInt(proxy.split(":")[2]);
            cc.setProxyHost(proxy);
            cc.setProxyPort(port);
        }

        if (client == null) {
            if (config.containsKey("role_arn")) {
                STSAssumeRoleSessionCredentialsProvider credentialsProvider = new STSAssumeRoleSessionCredentialsProvider(
                        (String) config.get("role_arn"),
                        "cloudwatch_exporter"
                );
                this.client = new AmazonCloudWatchClient(credentialsProvider,cc);
            } else {
                this.client = new AmazonCloudWatchClient(cc);
            }
            this.client.setEndpoint(getMonitoringEndpoint());

        } else {
            this.client = client;
        }

        if (!config.containsKey("metrics")) {
            throw new IllegalArgumentException("Must provide metrics");
        }
        for (Object ruleObject : (List<Map<String,Object>>) config.get("metrics")) {
            Map<String, Object> yamlMetricRule = (Map<String, Object>)ruleObject;
            MetricRule rule = new MetricRule();
            rules.add(rule);
            if (!yamlMetricRule.containsKey("aws_namespace") || !yamlMetricRule.containsKey("aws_metric_name")) {
                throw new IllegalArgumentException("Must provide aws_namespace and aws_metric_name");
            }
            rule.awsNamespace = (String)yamlMetricRule.get("aws_namespace");
            rule.awsMetricName = (String)yamlMetricRule.get("aws_metric_name");
            if (yamlMetricRule.containsKey("help")) {
                rule.help = (String)yamlMetricRule.get("help");
            }
            if (yamlMetricRule.containsKey("aws_dimensions")) {
                rule.awsDimensions = (List<String>)yamlMetricRule.get("aws_dimensions");
            }
            if (yamlMetricRule.containsKey("aws_dimension_select") && yamlMetricRule.containsKey("aws_dimension_select_regex")) {
                throw new IllegalArgumentException("Must not provide aws_dimension_select and aws_dimension_select_regex at the same time");
            }
            if (yamlMetricRule.containsKey("aws_dimension_select")) {
                rule.awsDimensionSelect = (Map<String, List<String>>)yamlMetricRule.get("aws_dimension_select");
            }
            if (yamlMetricRule.containsKey("aws_dimension_select_regex")) {
                rule.awsDimensionSelectRegex = (Map<String,List<String>>)yamlMetricRule.get("aws_dimension_select_regex");
            }
            if (yamlMetricRule.containsKey("aws_statistics")) {
                rule.awsStatistics = (List<String>)yamlMetricRule.get("aws_statistics");
            } else if (!yamlMetricRule.containsKey("aws_extended_statistics")) {
                rule.awsStatistics = new ArrayList(Arrays.asList("Sum", "SampleCount", "Minimum", "Maximum", "Average"));
            }
            if (yamlMetricRule.containsKey("aws_extended_statistics")) {
                rule.awsExtendedStatistics = (List<String>)yamlMetricRule.get("aws_extended_statistics");
            }
            if (yamlMetricRule.containsKey("period_seconds")) {
                rule.periodSeconds = ((Number)yamlMetricRule.get("period_seconds")).intValue();
            } else {
                rule.periodSeconds = defaultPeriod;
            }
            if (yamlMetricRule.containsKey("range_seconds")) {
                rule.rangeSeconds = ((Number)yamlMetricRule.get("range_seconds")).intValue();
            } else {
                rule.rangeSeconds = defaultRange;
            }
            if (yamlMetricRule.containsKey("delay_seconds")) {
                rule.delaySeconds = ((Number)yamlMetricRule.get("delay_seconds")).intValue();
            } else {
                rule.delaySeconds = defaultDelay;
            }
        }
    }

    public String getMonitoringEndpoint() {
        return "https://" + region.getServiceEndpoint("monitoring");
    }

    /**
     * Check if a metric should be used according to `aws_dimension_select` or `aws_dimension_select_regex`
     */
    private boolean useMetric(MetricRule rule, Metric metric) {
        if (rule.awsDimensionSelect == null && rule.awsDimensionSelectRegex == null) {
            return true;
        }
        if (rule.awsDimensionSelect != null  && metricsIsInAwsDimensionSelect(rule, metric)) {
            return true;
        }
        if (rule.awsDimensionSelectRegex != null  && metricIsInAwsDimensionSelectRegex(rule, metric)) {
            return true;
        }
        return false;
    }

    /**
     * Check if a metric is matched in `aws_dimension_select`
     */
    private boolean metricsIsInAwsDimensionSelect(MetricRule rule, Metric metric) {
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
    private boolean metricIsInAwsDimensionSelectRegex(MetricRule rule, Metric metric) {
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

    private Datapoint getNewestDatapoint(java.util.List<Datapoint> datapoints) {
        Datapoint newest = null;
        for (Datapoint d: datapoints) {
            if (newest == null || newest.getTimestamp().before(d.getTimestamp())) {
                newest = d;
            }
        }
        return newest;
    }

    private String toSnakeCase(String str) {
        return str.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
    }

    private String safeName(String s) {
        // Change invalid chars to underscore, and merge underscores.
        return s.replaceAll("[^a-zA-Z0-9:_]", "_").replaceAll("__+", "_");
    }

    private String help(MetricRule rule, String unit, String statistic) {
        if (rule.help != null) {
            return rule.help;
        }
        return "CloudWatch metric " + rule.awsNamespace + " " + rule.awsMetricName
                + " Dimensions: " + rule.awsDimensions + " Statistic: " + statistic
                + " Unit: " + unit;
    }

    private void scrape(List<MetricFamilySamples> mfs) {

        ExecutorService executor = Executors.newFixedThreadPool(10);

        long start = System.currentTimeMillis();
        for (MetricRule rule: rules) {

            Runnable worker = new CollectorWorker(mfs,start,rule,brokenDynamoMetrics,client,cloudwatchRequests);
            executor.execute(worker);
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }

    public List<MetricFamilySamples> collect() {
        long start = System.nanoTime();
        double error = 0;
        List<MetricFamilySamples> mfs = Collections.synchronizedList(new ArrayList<MetricFamilySamples>());

        try {
            scrape(mfs);
        } catch (Exception e) {
            error = 1;
            LOGGER.log(Level.WARNING, "CloudWatch scrape failed", e);
        }
        List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>();
        samples.add(new MetricFamilySamples.Sample(
                "cloudwatch_exporter_scrape_duration_seconds", new ArrayList<String>(), new ArrayList<String>(), (System.nanoTime() - start) / 1.0E9));
        mfs.add(new MetricFamilySamples("cloudwatch_exporter_scrape_duration_seconds", Type.GAUGE, "Time this CloudWatch scrape took, in seconds.", samples));

        samples = new ArrayList<MetricFamilySamples.Sample>();
        samples.add(new MetricFamilySamples.Sample(
                "cloudwatch_exporter_scrape_error", new ArrayList<String>(), new ArrayList<String>(), error));
        mfs.add(new MetricFamilySamples("cloudwatch_exporter_scrape_error", Type.GAUGE, "Non-zero if this scrape failed.", samples));
        return mfs;
    }

    /**
     * Convenience function to run standalone.
     */
    public static void main(String[] args) throws Exception {
        String region = "eu-west-1";
        if (args.length > 0) {
            region = args[0];
        }
        CloudWatchCollector jc = new CloudWatchCollector(("{"
                + "`region`: `" + region + "`,"
                + "`metrics`: [{`aws_namespace`: `AWS/ELB`, `aws_metric_name`: `RequestCount`, `aws_dimensions`: [`AvailabilityZone`, `LoadBalancerName`]}] ,"
                + "}").replace('`', '"'));
        for(MetricFamilySamples mfs : jc.collect()) {
            System.out.println(mfs);
        }
    }
}

