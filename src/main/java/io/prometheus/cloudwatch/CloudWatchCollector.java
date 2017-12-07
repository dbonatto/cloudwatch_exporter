package io.prometheus.cloudwatch;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.Metric;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class CloudWatchCollector extends Collector {
    private static final Logger LOGGER = Logger.getLogger(CloudWatchCollector.class.getName());
    private static final String CLOUDWATCH_EXPORTER_SCRAPE_ERROR = "cloudwatch_exporter_scrape_error";
    private static final String CLOUDWATCH_EXPORTER_SCRAPE_DURATION_SECONDS = "cloudwatch_exporter_scrape_duration_seconds";
    private Map<String, Object> config  = new HashMap<String, Object>();

    private AmazonCloudWatch client;
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

    private ArrayList<MetricRule> rules = new ArrayList<MetricRule>();

    CloudWatchCollector(Reader in) throws IOException {
        this((Map<String, Object>)new Yaml().load(in),null);
    }
    CloudWatchCollector(String yamlConfig) {
        this((Map<String, Object>)new Yaml().load(yamlConfig),null);
    }

    /* For unittests. */
    CloudWatchCollector(String jsonConfig, AmazonCloudWatchClient client) {
        this((Map<String, Object>)new Yaml().load(jsonConfig), client);
    }

    private CloudWatchCollector(Map<String, Object> config, AmazonCloudWatchClient client) {
        if(config != null) {  // Yaml config empty, set config to empty map.
            this.config = config;
        }
        if (!this.config.containsKey("region")) {
            throw new IllegalArgumentException("Must provide region");
        }
        region = RegionUtils.getRegion((String) this.config.get("region"));

        this.client = client;

//        createClient();

        int defaultPeriod = 60;
        if (this.config.containsKey("period_seconds")) {
            defaultPeriod = ((Number)this.config.get("period_seconds")).intValue();
        }
        int defaultRange = 600;
        if (this.config.containsKey("range_seconds")) {
            defaultRange = ((Number)this.config.get("range_seconds")).intValue();
        }
        int defaultDelay = 600;
        if (this.config.containsKey("delay_seconds")) {
            defaultDelay = ((Number)this.config.get("delay_seconds")).intValue();
        }

        if (!this.config.containsKey("metrics")) {
            throw new IllegalArgumentException("Must provide metrics");
        }
        for (Object ruleObject : (List<Map<String,Object>>) this.config.get("metrics")) {
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
                rule.awsStatistics = new ArrayList<String>(Arrays.asList("Sum", "SampleCount", "Minimum", "Maximum", "Average"));
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
    private static boolean regexListMatch(List<String> regexList, String input) {
        for (String regex: regexList) {
            if (Pattern.matches(regex, input)) {
                return true;
            }
        }
        return false;
    }

    private void scrape(List<MetricFamilySamples> mfs) throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(Configuration.threadPoolSize());
        AtomicInteger error = new AtomicInteger();

        // Todo: Refactor tests so this override is not needed anymore
        AmazonCloudWatch cloudWatch = this.client != null ? this.client : doCreateClient();

        try {
            for (MetricRule rule: rules) {
                Runnable worker = new CollectorWorker(mfs,rule,cloudwatchRequests, error, cloudWatch);
                executor.execute(worker);
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
            cloudWatch.shutdown();
        }

        if (error.intValue() > 0) {
            throw new Exception("Error Scraping metrics.");
        }
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
                CLOUDWATCH_EXPORTER_SCRAPE_DURATION_SECONDS, new ArrayList<String>(), new ArrayList<String>(), (System.nanoTime() - start) / 1.0E9));
        mfs.add(new MetricFamilySamples(CLOUDWATCH_EXPORTER_SCRAPE_DURATION_SECONDS, Type.GAUGE, "Time this CloudWatch scrape took, in seconds.", samples));

        samples = new ArrayList<MetricFamilySamples.Sample>();
        samples.add(new MetricFamilySamples.Sample(
                CLOUDWATCH_EXPORTER_SCRAPE_ERROR, new ArrayList<String>(), new ArrayList<String>(), error));
        mfs.add(new MetricFamilySamples(CLOUDWATCH_EXPORTER_SCRAPE_ERROR, Type.GAUGE, "Non-zero if this scrape failed.", samples));

        return mfs;
    }

    public String getMonitoringEndpoint() {
        return "https://" + region.getServiceEndpoint("monitoring");
    }

    private void createClient() {

        if (this.client == null) {
            this.client = doCreateClient();
        }
    }

    public AmazonCloudWatch doCreateClient() {
        AmazonCloudWatch client;

        ClientConfiguration cc = new ClientConfiguration();
        String proxy = System.getenv("http_proxy");

        if(proxy != null && !proxy.isEmpty()) {
            try {
                URL proxyUrl = new URL(proxy);

                cc.setProxyHost(proxyUrl.getHost());
                cc.setProxyPort(proxyUrl.getPort());
            } catch (MalformedURLException e) {
                LOGGER.log(Level.WARNING, "Proxy configuration is invalid.", e);
            }
        }

        cc.withMaxConnections(Configuration.maxConnections());
        cc.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicy());

        AwsClientBuilder.EndpointConfiguration ec = new AwsClientBuilder.EndpointConfiguration(
                getMonitoringEndpoint(), this.region.getName());

        if (this.config.containsKey("role_arn")) {
            STSAssumeRoleSessionCredentialsProvider cp = new STSAssumeRoleSessionCredentialsProvider.Builder(
                    (String) this.config.get("role_arn"),
                    "cloudwatch_exporter").build();

            client = AmazonCloudWatchClientBuilder.standard()
                    .withCredentials(cp).withClientConfiguration(cc).withEndpointConfiguration(ec).build();

        } else {
            client = AmazonCloudWatchClientBuilder.standard()
                    .withClientConfiguration(cc).withEndpointConfiguration(ec).build();
        }

        return client;
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

