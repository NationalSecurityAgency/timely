package main

type TimelyQueryForm struct {
    Metric               string            `json:"metric"`
    Alias                string            `json:"alias"`
    Aggregator           string            `json:"aggregator"`
    DisableDownsampling  bool              `json:"disableDownsampling"`
    DownsampleAggregator string            `json:"downsampleAggregator"`
    DownsampleFillPolicy string            `json:"downsampleFillPolicy"`
    DownsampleInterval   string            `json:"downsampleInterval"`
    Tags                 map[string]string `json:"tags"`
    Filters              []Filter          `json:"filters"`
    ShouldComputeRate    bool              `json:"shouldComputeRate"`
    IsCounter            bool              `json:"isCounter"`
    CounterMax           string            `json:"counterMax"`
    CounterResetValue    string            `json:"counterResetValue"`
    Tsuids               []string          `json:"tsuids"`
    QueryType            string            `json:"queryType"`
    RefId                string            `json:"refId"`
    Datasource           string            `json:"datasource"`
    DatasourceId         int64             `json:"datasourceId"`
    IntervalMs           int64             `json:"intervalMs"`
    MaxDataPoints        int64             `json:"maxDataPoints"`
    OrgId                int64             `json:"ordId"`
}

type TimelyQueryServer struct {
    Metric               string            `json:"metric"`
    Aggregator           string            `json:"aggregator"`
    DisableDownsampling  bool              `json:"disableDownsampling"`
    DownsampleAggregator string            `json:"downsampleAggregator"`
    DownsampleFillPolicy string            `json:"downsampleFillPolicy"`
    DownsampleInterval   string            `json:"downsampleInterval"`
    Tags                 map[string]string `json:"tags"`
    Filters              []Filter          `json:"filters"`
    ShouldComputeRate    bool              `json:"shouldComputeRate"`
    IsCounter            bool              `json:"isCounter"`
    CounterMax           string            `json:"counterMax"`
    CounterResetValue    string            `json:"counterResetValue"`
    Tsuids               []string          `json:"tsuids"`
    QueryType            string            `json:"queryType"`
    RefId                string            `json:"refId"`
}

type Filter struct {
    Type    string `json:"type"`
    TagK    string `json:"tagk"`
    Filter  string `json:"filter"`
    GroupBy bool   `json:"groupBy"`
}

type RateOptions struct {
    Counter    bool  `json:"counter"`
    CounterMax int32 `json:"counterMax"`
    ResetValue int32 `json:"resetValue"`
}

type TimelyQuery struct {
    Metric      string            `json:"metric"`
    Aggregator  string            `json:"aggregator"`
    Rate        bool              `json:"rate"`
    RateOptions RateOptions       `json:"rateOptions"`
    Downsample  string            `json:"downsample"`
    Tags        map[string]string `json:"tags"`
    Filters     []Filter          `json:"filters"`
    Tsuids      []string          `json:"tsuids"`
}

type TimelyRequest struct {
    MsResolution      bool          `json:"msResolution"`
    GlobalAnnotations bool          `json:"globalAnnotations"`
    Start             int64         `json:"start"`
    End               int64         `json:"end"`
    Queries           []TimelyQuery `json:"queries"`
}

type TimelyResponse struct {
    Metric         string            `json:"metric"`
    Tags           map[string]string `json:"tags"`
    AggregatedTags []string          `json:"aggregatedTags"`
    DataPoints     map[int64]float64 `json:"dps"`
}

type TimelyDataSourceOptions struct {
    TimelyHost                      string `json:"timelyHost"`
    HttpsPort                       string `json:"httpsPort"`
    OauthPassThru                   bool `json:"oauthPassThru"`
    UseClientCertWhenOAuthMissing   bool `json:"useClientCertWhenOAuthMissing"`
    ClientCertificatePath           string `json:"clientCertificatePath"`
    ClientKeyPath                   string `json:"clientKeyPath"`
    CertificateAuthorityPath        string `json:"certificateAuthorityPath"`
    AllowInsecureSsl                bool `json:"allowInsecureSsl"`
}

type TimelyErrorResponse struct {
    ResponseCode                    int32 `json:responseCode`
    Message                         string `json:message`
    DetailMessage                   string `json:detailMessage`
}