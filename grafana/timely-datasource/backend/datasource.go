package backend

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"net/url"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces - only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*TimelyDatasource)(nil)
	_ backend.CheckHealthHandler    = (*TimelyDatasource)(nil)
	_ instancemgmt.InstanceDisposer = (*TimelyDatasource)(nil)
)

var (
	logger = hclog.New(&hclog.LoggerOptions{
		// Use debug as level since anything less severe is suppressed.
		Level: hclog.Debug,
		// Use JSON format to make the output in Grafana format and work
		// when using multiple arguments such as Debug("message", "key", "value").
		JSONFormat: true,
		Name:       "timely",
	})
)

type TimelyDatasource struct {
	im                       instancemgmt.InstanceManager
	Certificate              *tls.Certificate
	CertPool                 *x509.CertPool
	ClientCertificatePath    string
	ClientKeyPath            string
	CertificateAuthorityPath string
	Mutex                    sync.Mutex
}

//type TimelyDatasourceInstanceSettings struct {
//}

// NewDatasource creates a new datasource instance.
func NewDatasource(_ context.Context, _ backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	return &TimelyDatasource{}, nil
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (td *TimelyDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = ""

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func (td *TimelyDatasource) CallResource(ctx context.Context, request *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {

	if request.Body == nil {
		logger.Debug("CallResource", "Path", request.Path, "URL", request.URL)
	} else {
		logger.Debug("CallResource", "Path", request.Path, "URL", request.URL, "Body", string(request.Body))
	}
	var response backend.CallResourceResponse
	pluginContext := request.PluginContext
	var httpResponse *http.Response
	var timelyDataSourceOptions TimelyDataSourceOptions
	err := json.Unmarshal(pluginContext.DataSourceInstanceSettings.JSONData, &timelyDataSourceOptions)
	if err != nil {
		logger.Error("Error unmarshalling datasource JSONData", "error", err)
		return err
	}

	httpClient, err := td.GetHttpClient(timelyDataSourceOptions, true)
	if err != nil {
		logger.Error("Error getting httpClient", "error", err)
		return err
	}
	if strings.HasPrefix(request.Path, "/api/aggregators") ||
		strings.HasPrefix(request.Path, "/api/suggest") ||
		strings.HasPrefix(request.Path, "/api/search/lookup") {

		var timelyUrl *url.URL
		if request.Method == "GET" {
			var x = strings.Index(request.URL, request.Path)
			finalParams := ""
			if x > -1 {
				params := request.URL[x+len(request.Path) : len(request.URL)]

				if len(params) > 0 && strings.HasPrefix(params, "?") {
					finalParams = params
				}
			}
			timelyUrl, err = url.Parse("https://" + timelyDataSourceOptions.TimelyHost + ":" + strconv.Itoa(int(timelyDataSourceOptions.HttpsPort)) + request.Path + finalParams)
			if err != nil {
				return err
			}
			httpResponse, err = httpClient.Get(timelyUrl.String())
		} else if request.Method == "POST" {
			timelyUrl, err = url.Parse("https://" + timelyDataSourceOptions.TimelyHost + ":" + strconv.Itoa(int(timelyDataSourceOptions.HttpsPort)) + request.Path)
			if err != nil {
				return err
			}
			httpResponse, err = httpClient.Post(timelyUrl.String(), "application/json", bytes.NewReader(request.Body))
		}

		if err != nil {
			logger.Error("Error fetching resource", "url", timelyUrl.String(), "error", err)
			return err
		}
		if httpResponse == nil {
			logger.Error("Error fetching resource, httpResponse null", "url", timelyUrl.String())
			return fmt.Errorf("Error fetching resource url:%s, httpResponse null", timelyUrl.String())
		}

		if httpResponse.StatusCode != 200 {
			logger.Error("Error fetching resource", "url", timelyUrl.String(), "responseCode", httpResponse.StatusCode)
			return fmt.Errorf("Error fetching resource url:%s responseCode:%d", timelyUrl.String(), httpResponse.StatusCode)
		}

		bytes, err := io.ReadAll(httpResponse.Body)
		if err != nil {
			logger.Error("Error reading resource", "url", timelyUrl, "error", err)
			return err
		}
		response.Status = 200
		response.Body = bytes
	} else {
		// no matching resource
		response.Status = 404
	}
	sender.Send(&response)
	return nil
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifer).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (td *TimelyDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	logger.Info("QueryData called with:", "Request", req)
	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res, error := td.Query(req, q)
		if error != nil {
			errorResponse := backend.DataResponse{
				Error: error,
			}
			response.Responses[q.RefID] = errorResponse
		} else {
			// save the response in a hashmap
			// based on with RefID as identifier
			response.Responses[q.RefID] = *res
		}
	}
	return response, nil
}

func (td *TimelyDatasource) Query(queryDataRequest *backend.QueryDataRequest, dataQuery backend.DataQuery) (*backend.DataResponse, error) {

	timelyRequest := TimelyRequest{
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
			timelyQueryForm = TimelyQueryForm{
				Metric:               timelyQueryServer.Metric,
				Aggregator:           timelyQueryServer.Aggregator,
				DisableDownsampling:  timelyQueryServer.DisableDownsampling,
				DownsampleAggregator: timelyQueryServer.DownsampleAggregator,
				DownsampleFillPolicy: timelyQueryServer.DownsampleFillPolicy,
				DownsampleInterval:   timelyQueryServer.DownsampleInterval,
				Tags:                 timelyQueryServer.Tags,
				Filters:              timelyQueryServer.Filters,
				ShouldComputeRate:    timelyQueryServer.ShouldComputeRate,
				RateInterval:         timelyQueryServer.RateInterval,
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

	var timelyDatasourceOptions TimelyDataSourceOptions

	err = json.Unmarshal(queryDataRequest.PluginContext.DataSourceInstanceSettings.JSONData, &timelyDatasourceOptions)
	if err != nil {
		logger.Error("Error unmarshalling datasource JSONData", "error", err)
		return nil, err
	}

	timelyQuery := td.convertTimelyQuery(&timelyQueryForm, timelyDatasourceOptions)
	timelyRequest.Queries = append(timelyRequest.Queries, *timelyQuery)

	logger.Debug("Query", "timelyRequest", timelyRequest)

	var httpRequest *http.Request
	httpRequest, err = td.createRequest(timelyDatasourceOptions, timelyRequest)
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
	httpClient, err := td.GetHttpClient(timelyDatasourceOptions, useClientCertIfAvailable)
	if err != nil {
		return nil, err
	}

	res, err := httpClient.Do(httpRequest)
	if err != nil {
		logger.Error("Error executing query", "error", err)
		return nil, err
	}
	return td.parseResponse(res)
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *TimelyDatasource) Dispose() {
	// Clean up datasource instance resources.
}

func (td *TimelyDatasource) createRequest(tdo TimelyDataSourceOptions, timelyRequest TimelyRequest) (*http.Request, error) {
	u, err := url.Parse("https://" + tdo.TimelyHost + ":" + strconv.Itoa(int(tdo.HttpsPort)) + "/api/query")
	if err != nil {
		logger.Error("Failed creating query url", "error", err)
		return nil, fmt.Errorf("Failed creating query url. error: %v", err)
	}
	postData, err := json.Marshal(timelyRequest)
	if err != nil {
		logger.Error("Failed marshaling timelyRequest", "error", err)
		return nil, fmt.Errorf("Failed to create request. error: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(postData))
	if err != nil {
		logger.Error("Failed to create request", "error", err)
		return nil, fmt.Errorf("Failed to create request. error: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	requestData, err := json.Marshal(timelyRequest)
	if err != nil {
		logger.Error("Failed marshaling request", "error", err)
	} else {
		logger.Debug("Created query request: " + string(requestData))
	}
	return req, err
}

func (td *TimelyDatasource) parseResponse(res *http.Response) (*backend.DataResponse, error) {

	body, err := io.ReadAll(res.Body)
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
		var name = val.Metric
		for k, v := range val.Tags {
			name = name + " " + k + "=" + v
		}
		frame := data.NewFrame(name)
		timestamps := make([]*time.Time, 0)
		values := make([]*float64, 0)
		keys := make([]int64, 0, len(val.DataPoints))
		for k := range val.DataPoints {
			keys = append(keys, k)
		}

		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})

		for _, timestampInt64 := range keys {
			value := val.DataPoints[timestampInt64]
			timestampTime := time.Unix(timestampInt64/1000, 0)
			timestamps = append(timestamps, &timestampTime)
			newValue := float64(value)
			values = append(values, &newValue)
		}
		frame.Fields = append(frame.Fields, data.NewField("time", val.Tags, timestamps))
		frame.Fields = append(frame.Fields, data.NewField(val.Metric, val.Tags, values))
		response.Frames = append(response.Frames, frame)
	}
	// add the frames to the response
	return &response, nil
}

func (td *TimelyDatasource) convertTimelyQuery(timelyQueryForm *TimelyQueryForm, datasourceOptions TimelyDataSourceOptions) *TimelyQuery {

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

	for i := 0; i < len(timelyQueryForm.DatasourceTags); i++ {
		tagKey := timelyQueryForm.DatasourceTags[i]
		if _, ok := datasourceOptions.DatasourceTags[tagKey]; ok {
			timelyQuery.Tags[tagKey] = datasourceOptions.DatasourceTags[tagKey]
		}
	}

	// future work - replace scopedVariables in metric, tags, filters, aggregator

	timelyQuery.Aggregator = "avg"

	if timelyQueryForm.Aggregator != "" {
		timelyQuery.Aggregator = timelyQueryForm.Aggregator
	}

	if timelyQueryForm.ShouldComputeRate {
		timelyQuery.Rate = true
		timelyQuery.RateOptions.Interval = timelyQueryForm.RateInterval
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

func (td *TimelyDatasource) clientSslSettingsChanged(settings TimelyDataSourceOptions) bool {

	clientCertificatePath := settings.ClientCertificatePath
	clientKeyPath := settings.ClientKeyPath
	certificateAuthorityPath := settings.CertificateAuthorityPath
	return clientCertificatePath != td.ClientCertificatePath || clientKeyPath != td.ClientKeyPath ||
		certificateAuthorityPath != td.CertificateAuthorityPath
}

func (td *TimelyDatasource) GetHttpClient(settings TimelyDataSourceOptions, useClientCertIfAvailable bool) (*http.Client, error) {

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

	clientCertificatePath := settings.ClientCertificatePath
	clientKeyPath := settings.ClientKeyPath
	certificateAuthorityPath := settings.CertificateAuthorityPath

	if td.clientSslSettingsChanged(settings) {
		td.Mutex.Lock()
		// check again to prevent double initialization
		if td.clientSslSettingsChanged(settings) {
			// if clientCertificatePath or clientKeyPath changed, then re-read the
			// client cert into the datasource and also save the new paths
			if clientCertificatePath != td.ClientCertificatePath || clientKeyPath != td.ClientKeyPath {
				if clientCertificatePath != "" && clientKeyPath != "" {
					cert, err := tls.LoadX509KeyPair(clientCertificatePath, clientKeyPath)
					if err != nil {
						logger.Error("Failed to setup client certificate", "cert_path", clientCertificatePath, "key_path", clientKeyPath, "error", err)
						return nil, fmt.Errorf("Failed to setup client certificate")
					}
					td.Certificate = &cert
					logger.Debug("Completed setup of client certificate", "cert_path", clientCertificatePath, "key_path", clientKeyPath)
				} else {
					// if one but not both items have been configured
					if clientCertificatePath != "" || clientKeyPath != "" {
						logger.Info("Both cert_path and key_path are required. Client connection updated with no client certificate.")
					}
					td.Certificate = nil
				}
				td.ClientCertificatePath = clientCertificatePath
				td.ClientKeyPath = clientKeyPath
			}

			// if certificateAuthorityPath changed, then re-read the
			// certificate authority into the datasource and also save the new path
			if certificateAuthorityPath != td.CertificateAuthorityPath {
				if certificateAuthorityPath != "" {
					caCert, err := ioutil.ReadFile(certificateAuthorityPath)
					if err != nil {
						logger.Error("Failed to setup certificate authority", "ca_path", certificateAuthorityPath, "error", err)
						return nil, fmt.Errorf("Failed to setup certificate authority")
					}
					td.CertPool = x509.NewCertPool()
					td.CertPool.AppendCertsFromPEM(caCert)
					logger.Debug("Completed setup of certificate authority", "ca_path", certificateAuthorityPath)
				} else {
					logger.Debug("Client connection updated with no certificate authority")
					td.CertPool = nil
				}
				td.CertificateAuthorityPath = certificateAuthorityPath
			}
		}
		td.Mutex.Unlock()
	}

	// protect against changes in-progress
	td.Mutex.Lock()
	if useClientCertIfAvailable && td.Certificate != nil {
		tr.TLSClientConfig.Certificates = append(tr.TLSClientConfig.Certificates, *td.Certificate)
	}
	if td.CertPool != nil {
		tr.TLSClientConfig.RootCAs = td.CertPool
	}
	td.Mutex.Unlock()
	return httpClient, nil
}
