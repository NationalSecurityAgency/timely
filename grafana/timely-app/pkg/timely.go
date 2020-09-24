package main

import (
    "bytes"
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "github.com/grafana/grafana-plugin-sdk-go/backend"
    "github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
    "github.com/grafana/grafana-plugin-sdk-go/data"
    "github.com/hashicorp/go-hclog"
    "sort"
    "strconv"
    "time"

    "encoding/json"
    "io/ioutil"
    "net/http"
    "net/url"
)

var (
    logger = hclog.New(&hclog.LoggerOptions{
        // Use debug as level since anything less severe is suppressed.
        Level: hclog.Debug,
        // Use JSON format to make the output in Grafana format and work
        // when using multiple arguments such as Debug("message", "key", "value").
        JSONFormat: true,
        Name: "timely",
    })
)

func init() {
    http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
}

// TimelyDatasource is an example datasource used to scaffold
// new datasource plugins with an backend.
type TimelyDatasource struct {
    // The instance manager can help with lifecycle management
    // of datasource instances in plugins. It's not a requirements
    // but a best practice that we recommend that you follow.
    im instancemgmt.InstanceManager
}

func (dataSource *TimelyDatasource) Query(queryDataRequest *backend.QueryDataRequest, dataQuery backend.DataQuery) (*backend.DataResponse, error) {

    timelyRequest := TimelyRequest {
        MsResolution:      true,
        GlobalAnnotations: true,
        Start:             dataQuery.TimeRange.From.Unix() * 1000,
        End:               dataQuery.TimeRange.To.Unix() * 1000,
    }

    var timelyQueryForm TimelyQueryForm
    var timelyQueryServer TimelyQueryServer
    var err error
    var isUser bool

    if queryDataRequest.PluginContext.User == nil {
        isUser = false
        err = json.Unmarshal(dataQuery.JSON, &timelyQueryServer)
        if err == nil {
            timelyQueryForm = TimelyQueryForm {
                Metric:               timelyQueryServer.Metric,
                Aggregator:           timelyQueryServer.Aggregator,
                DisableDownsampling:  timelyQueryServer.DisableDownsampling,
                DownsampleAggregator: timelyQueryServer.DownsampleAggregator,
                DownsampleFillPolicy: timelyQueryServer.DownsampleFillPolicy,
                DownsampleInterval:   timelyQueryServer.DownsampleInterval,
                Tags:                 timelyQueryServer.Tags,
                Filters:              timelyQueryServer.Filters,
                ShouldComputeRate:    timelyQueryServer.ShouldComputeRate,
                IsCounter:            timelyQueryServer.IsCounter,
                CounterMax:           timelyQueryServer.CounterMax,
                CounterResetValue:    timelyQueryServer.CounterResetValue,
                Tsuids:               timelyQueryServer.Tsuids,
                QueryType:            timelyQueryServer.QueryType,
                RefId:                timelyQueryServer.RefId,
            }
        }
    } else {
        isUser = true
        err = json.Unmarshal(dataQuery.JSON, &timelyQueryForm)
    }

    if err != nil {
        logger.Error("Failed parsing timelyQuery", "error", err)
        return nil, err
    }

    timelyQuery := dataSource.convertTimelyQuery(&timelyQueryForm)
    timelyRequest.Queries = append(timelyRequest.Queries, *timelyQuery)

    logger.Debug("Query", "timelyRequest", timelyRequest)

    var timelyDatasourceOptions TimelyDataSourceOptions

    err = json.Unmarshal(queryDataRequest.PluginContext.DataSourceInstanceSettings.JSONData, &timelyDatasourceOptions)
    if err != nil {
        logger.Error("Error unmarshalling datasource JSONData", "error", err)
        return nil, err
    }

    var httpRequest *http.Request
    httpRequest, err = dataSource.createRequest(timelyDatasourceOptions, timelyRequest)
    if err != nil {
        logger.Error("Error creating httpRequest", "error", err)
        return nil, err
    }

    oauthToken, tokenExists := queryDataRequest.Headers["Authorization"]
    if tokenExists {
        logger.Debug("Adding Authorization:" + oauthToken)
        httpRequest.Header.Add("Authorization", oauthToken)
    }

    var useClientCertIfAvailable = !isUser || (!tokenExists && timelyDatasourceOptions.UseClientCertWhenOAuthMissing)
    httpClient, err := GetHttpClient(timelyDatasourceOptions, useClientCertIfAvailable)
    if err != nil {
        return nil, err
    }

    res, err := httpClient.Do(httpRequest)
    if err != nil {
        logger.Error("Error executing query", "error", err)
        return nil, err
    }
    return dataSource.parseResponse(res)
}

func (e *TimelyDatasource) createRequest(tdo TimelyDataSourceOptions, data TimelyRequest) (*http.Request, error) {
    u, err := url.Parse("https://" + tdo.TimelyHost + ":" + tdo.HttpsPort + "/api/query")
    if err != nil {
        logger.Error("Failed creating query url", "error", err)
        return nil, fmt.Errorf("Failed creating query url. error: %v", err)
    }
    postData, err := json.Marshal(data)
    if err != nil {
        logger.Error("Failed marshaling data", "error", err)
        return nil, fmt.Errorf("Failed to create request. error: %v", err)
    }
    req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(postData))
    if err != nil {
        logger.Error("Failed to create request", "error", err)
        return nil, fmt.Errorf("Failed to create request. error: %v", err)
    }
    req.Header.Set("Content-Type", "application/json")

    requestData, err := json.Marshal(data)
    if err != nil {
        logger.Error("Failed marshaling request", "error", err)
    } else {
        logger.Info("Created query request: " + string(requestData))
    }
    return req, err
}

func (e *TimelyDatasource) parseResponse(res *http.Response) (*backend.DataResponse, error) {

    body, err := ioutil.ReadAll(res.Body)
    defer res.Body.Close()
    if err != nil {
        return nil, err
    }

    if res.StatusCode/100 != 2 {
        var errorResponse TimelyErrorResponse
        err = json.Unmarshal(body, &errorResponse)
        if err != nil {
            logger.Error("Query failed", "status", res.Status, "body", string(body))
            return nil, fmt.Errorf("Query failed - %v: %s", res.Status, string(body))
        }
        logger.Error("Query failed", "status", res.Status, "message", errorResponse.Message, "detailMessage", errorResponse.DetailMessage)
        return nil, fmt.Errorf("Query failed - %v [%s] [%s]", res.Status, errorResponse.Message, errorResponse.DetailMessage)
    }

    var object []TimelyResponse
    err = json.Unmarshal(body, &object)
    if err != nil {
        return nil, fmt.Errorf("Failed to unmarshal Timely response - body: %s status: %v", string(body), res.Status)
    }

    response := backend.DataResponse{}

    for _, val := range object {
        var name = val.Metric;
        for k, v := range val.Tags {
            name = name + " " + k + "=" + v;
        }
        frame := data.NewFrame(name)
        timestamps := make([]*time.Time, 0)
        values := make([]*float64, 0)
        keys := make([]int64, 0, len(val.DataPoints));
        for k := range val.DataPoints {
            keys = append(keys, k);
        }

        sort.Slice(keys, func(i, j int) bool {
            return keys[i] < keys[j];
        });

        for _, timestampInt64 := range keys {
            value := val.DataPoints[timestampInt64];
            timestampTime := time.Unix(timestampInt64/1000, 0);
            timestamps = append(timestamps, &timestampTime)
            newValue := float64(value);
            values = append(values, &newValue)
        }
        frame.Fields = append(frame.Fields, data.NewField("time", val.Tags, timestamps))
        frame.Fields = append(frame.Fields, data.NewField(val.Metric, val.Tags, values))
        response.Frames = append(response.Frames, frame)
    }
    // add the frames to the response
    return &response, nil
}

func (e *TimelyDatasource) convertTimelyQuery(timelyQueryForm *TimelyQueryForm) *TimelyQuery {

    if timelyQueryForm == nil {
        return nil
    }
    timelyQuery := TimelyQuery{
        Metric:      timelyQueryForm.Metric,
        Aggregator:  "none",
        Rate:        false,
        RateOptions: RateOptions{},
        Downsample:  "60000ms-avg",
        Tags:        timelyQueryForm.Tags,
        Filters:     timelyQueryForm.Filters,
        Tsuids:      timelyQueryForm.Tsuids,
    }

    // future work - replace scopedVariables in metric, tags, filters, aggregator

    timelyQuery.Aggregator = "avg"

    if timelyQueryForm.Aggregator != "" {
        timelyQuery.Aggregator = timelyQueryForm.Aggregator
    }

    if timelyQueryForm.ShouldComputeRate {
        timelyQuery.Rate = true
        timelyQuery.RateOptions.Counter = timelyQueryForm.IsCounter
    }

    if len(timelyQueryForm.CounterMax) > 0 {
        counterMax, error := strconv.ParseInt(timelyQueryForm.CounterMax, 10, 32)
        if error == nil {
            timelyQuery.RateOptions.CounterMax = int32(counterMax)
        }
    }

    if len(timelyQueryForm.CounterResetValue) > 0 {
        counterResetValue, error := strconv.ParseInt(timelyQueryForm.CounterResetValue, 10, 32)
        if error == nil {
            timelyQuery.RateOptions.ResetValue = int32(counterResetValue)
        }
    }

    if !timelyQueryForm.DisableDownsampling {
        downsampleDuration, error := time.ParseDuration(timelyQueryForm.DownsampleInterval)
        if error == nil {
            timelyQuery.Downsample = strconv.FormatInt(downsampleDuration.Milliseconds(), 10) + "ms-" + timelyQueryForm.DownsampleAggregator
        }

        if len(timelyQueryForm.DownsampleFillPolicy) > 0 && timelyQueryForm.DownsampleFillPolicy != "none" {
            timelyQuery.Downsample += "-" + timelyQueryForm.DownsampleFillPolicy
        }
    }

    return &timelyQuery
}

func GetHttpClient(settings TimelyDataSourceOptions, useClientCertIfAvailable bool) (*http.Client, error) {
    // handle call back
    logger.Debug("Getting client", "allowInsecureSsl", strconv.FormatBool(settings.AllowInsecureSsl))
    tr := &http.Transport{
        Proxy: http.ProxyFromEnvironment,
        TLSClientConfig: &tls.Config{
            InsecureSkipVerify: settings.AllowInsecureSsl,
        },
    }
    httpClient := &http.Client{
        Transport: tr,
    }

    logger.Debug("Getting httpClient", "useClientCertIfAvailable", strconv.FormatBool(useClientCertIfAvailable))
    if useClientCertIfAvailable {
        if settings.ClientCertificatePath != "" || settings.ClientKeyPath != "" {
            cert, err := tls.LoadX509KeyPair(settings.ClientCertificatePath, settings.ClientKeyPath)
            if err != nil {
                logger.Error("Failed to setup client certificate", "cert_path", settings.ClientCertificatePath, "key_path", settings.ClientKeyPath, "error", err)
                return nil, fmt.Errorf("Failed to setup client certificate")
            }
            logger.Debug("Adding certificate", "ca_path", settings.ClientCertificatePath, "key_path", settings.ClientKeyPath)
            tr.TLSClientConfig.Certificates = append(tr.TLSClientConfig.Certificates, cert)
        }
    }

    if settings.CertificateAuthorityPath != "" {
        caCert, err := ioutil.ReadFile(settings.CertificateAuthorityPath)
        if err != nil {
            logger.Error("Failed to setup certificate authority", "ca_path", settings.CertificateAuthorityPath, "error", err)
            return nil, fmt.Errorf("Failed to setup certificate authority")
        }
        logger.Debug("Adding CA", "ca_path", settings.CertificateAuthorityPath)
        caCertPool := x509.NewCertPool()
        caCertPool.AppendCertsFromPEM(caCert)
        tr.TLSClientConfig.RootCAs = caCertPool
    }
    return httpClient, nil
}
