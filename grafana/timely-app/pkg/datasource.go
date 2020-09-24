package main

import (
    "context"
    "github.com/grafana/grafana-plugin-sdk-go/backend"
    "github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
    "github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
)

type TimelyDatasourceInstanceSettings struct {

}

func newDataSourceInstance(setting backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
    return &TimelyDatasourceInstanceSettings{

    }, nil
}

func (s *TimelyDatasourceInstanceSettings) Dispose() {
    // Called before creating a new instance to allow plugin authors
    // to cleanup.
}

func getServeOpts() datasource.ServeOpts {
    // creates a instance manager for your plugin. The function passed
    // into `NewInstanceManger` is called when the instance is created
    // for the first time or when a datasource configuration changed.
    im := datasource.NewInstanceManager(newDataSourceInstance)
    ds := &TimelyDatasource{
        im: im,
    }

    return datasource.ServeOpts{
        QueryDataHandler:    ds,
        CheckHealthHandler:  ds,
        CallResourceHandler: ds,
    }
}

func (ds *TimelyDatasource) getSettings(pluginContext backend.PluginContext) (*TimelyDatasourceInstanceSettings, error) {
    iface, err := ds.im.Get(pluginContext)
    if err != nil {
        return nil, err
    }

    return iface.(*TimelyDatasourceInstanceSettings), nil
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifer).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (td *TimelyDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
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
