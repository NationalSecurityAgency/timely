package main

import (
    "bytes"
    "context"
    "encoding/json"
    "github.com/grafana/grafana-plugin-sdk-go/backend"
    "io/ioutil"
    "net/http"
    "net/url"
    "strings"
)

func (ds *TimelyDatasource) CallResource(ctx context.Context, request *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {

    if (request.Body == nil) {
        logger.Debug("CallResource", "Path", request.Path, "URL", request.URL)
    } else {
        logger.Debug("CallResource", "Path", request.Path, "URL", request.URL, "Body", string(request.Body))
    }
    var response backend.CallResourceResponse
    pluginContext := request.PluginContext
    var httpResponse *http.Response;
    settings, err := ds.getSettings(pluginContext)
    if err != nil {
        logger.Error("Error getting datasource settings", "error", err)
        return err
    }
    var tdo TimelyDataSourceOptions
    err = json.Unmarshal(pluginContext.DataSourceInstanceSettings.JSONData, &tdo)
    if err != nil {
        logger.Error("Error unmarshalling datasource JSONData", "error", err)
        return err
    }

    if strings.HasPrefix(request.Path, "/api/aggregators") ||
        strings.HasPrefix(request.Path, "/api/suggest") ||
        strings.HasPrefix(request.Path, "/api/search/lookup") {

        if (request.Method == "GET") {
            var x = strings.Index(request.URL, request.Path);
            finalParams := "";
            if (x > -1) {
                params := request.URL[x+len(request.Path) : len(request.URL)];

                if (len(params) > 0 && strings.HasPrefix(params, "?")) {
                    finalParams = params;
                }
            }
            url, err := url.Parse("https://" + tdo.TimelyHost + ":" + tdo.HttpsPort + request.Path + finalParams);
            if err != nil {
                return err
            }
            httpResponse, err = settings.httpClient.Get(url.String())
        } else if (request.Method == "POST") {
            url, err := url.Parse("https://" + tdo.TimelyHost + ":" + tdo.HttpsPort + request.Path);
            if err != nil {
                return err
            }
            httpResponse, err = settings.httpClient.Post(url.String(), "application/json", bytes.NewReader(request.Body))
        }

        if err != nil {
            logger.Error("Error calling for resource", "Path", request.Path, "error", err)
            return err
        }
        bytes, err := ioutil.ReadAll(httpResponse.Body)
        if err != nil {
            logger.Error("Error reading resource", "Path", request.Path, "error", err)
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
