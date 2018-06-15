'use strict';

System.register(['lodash', 'angular', '../../../app/core/utils/datemath'], function (_export, _context) {
  "use strict";

  var _, angular, dateMath, _createClass, TimelyDatasource;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_lodash) {
      _ = _lodash.default;
    }, function (_angular) {
      angular = _angular.default;
    }, function (_appCoreUtilsDatemath) {
      dateMath = _appCoreUtilsDatemath;
    }],
    execute: function () {
      _createClass = function () {
        function defineProperties(target, props) {
          for (var i = 0; i < props.length; i++) {
            var descriptor = props[i];
            descriptor.enumerable = descriptor.enumerable || false;
            descriptor.configurable = true;
            if ("value" in descriptor) descriptor.writable = true;
            Object.defineProperty(target, descriptor.key, descriptor);
          }
        }

        return function (Constructor, protoProps, staticProps) {
          if (protoProps) defineProperties(Constructor.prototype, protoProps);
          if (staticProps) defineProperties(Constructor, staticProps);
          return Constructor;
        };
      }();

      _export('TimelyDatasource', TimelyDatasource = function () {

        // arguments injected by angular
        function TimelyDatasource(instanceSettings, $q, backendSrv, templateSrv, contextSrv) {
          _classCallCheck(this, TimelyDatasource);

          this.type = instanceSettings.type;
          this.name = instanceSettings.name;

          this.jsonData = instanceSettings.jsonData || {};
          this.httpsPort = instanceSettings.jsonData.httpsPort;
          this.timelyHost = instanceSettings.jsonData.timelyHost;
          this.wsPort = instanceSettings.jsonData.wsPort;
          if (!instanceSettings.jsonData.basicAuths) {
            instanceSettings.jsonData.basicAuths = false;
          }

          this.url = this._makeHttpsUrl();
          this.withCredentials = true;
          this.basicAuth = false;

          this.supportMetrics = true;
          this.tagKeys = {};

          this.$q = $q;
          this.backendSrv = backendSrv;
          this.templateSrv = templateSrv;
          this.contextSrv = contextSrv;

          this.aggregatorsPromise = null;
          this.filterTypesPromise = null;
        }

        // REQUIRED INTERFACE


        _createClass(TimelyDatasource, [{
          key: 'query',
          value: function query(options) {
            var start = this.convertToTSDBTime(options.rangeRaw.from, false);
            var end = this.convertToTSDBTime(options.rangeRaw.to, true);
            var qs = [];

            _.each(options.targets, function (target) {
              if (!target.metric) {
                return;
              }
              qs.push(this.convertTargetToQuery(target, options));
            }.bind(this));

            var queries = _.compact(qs);

            // No valid targets, return the empty result to save a round trip.
            if (_.isEmpty(queries)) {
              var d = this.$q.defer();
              d.resolve({ data: [] });
              return d.promise;
            }

            var groupByTags = {};
            _.each(queries, function (query) {
              if (query.filters && query.filters.length > 0) {
                _.each(query.filters, function (val) {
                  groupByTags[val.tagk] = true;
                });
              } else {
                _.each(query.tags, function (val, key) {
                  groupByTags[key] = true;
                });
              }
            });

            return this.performTimeSeriesQuery(queries, start, end).then(function (response) {
              var metricToTargetMapping = this.mapMetricsToTargets(response.data, options);
              var result = _.map(response.data, function (metricData, index) {
                index = metricToTargetMapping[index];
                if (index === -1) {
                  index = 0;
                }
                this._saveTagKeys(metricData);

                return this.transformMetricData(metricData, groupByTags, options.targets[index], options);
              }.bind(this));
              return { data: result };
            }.bind(this));
          }
        }, {
          key: 'testDatasource',
          value: function testDatasource() {
            return this.backendSrv.datasourceRequest({
              url: this.url + '/version',
              method: 'GET'
            }).then(function (response) {
              if (response.status === 200) {
                return { status: "success", message: "Data source is working", title: "Success" };
              }
            });
          }
        }, {
          key: 'annotationQuery',
          value: function annotationQuery(options) {
            var start = convertToTSDBTime(options.rangeRaw.from, false);
            var end = convertToTSDBTime(options.rangeRaw.to, true);
            var qs = [];
            var eventList = [];

            qs.push({ aggregator: "sum", metric: options.annotation.target });

            var queries = _.compact(qs);

            return this.performTimeSeriesQuery(queries, start, end).then(function (results) {
              if (results.data[0]) {
                var annotationObject = results.data[0].annotations;
                if (options.annotation.isGlobal) {
                  annotationObject = results.data[0].globalAnnotations;
                }
                if (annotationObject) {
                  _.each(annotationObject, function (annotation) {
                    var event = {
                      title: annotation.description,
                      time: Math.floor(annotation.startTime) * 1000,
                      text: annotation.notes,
                      annotation: options.annotation
                    };

                    eventList.push(event);
                  });
                }
              }
              return eventList;
            }.bind(this));
          }
        }, {
          key: 'metricFindQuery',
          value: function metricFindQuery(query) {
            if (!query) {
              return this.$q.when([]);
            }

            var interpolated;
            try {
              interpolated = this.templateSrv.replace(query);
            } catch (err) {
              return this.$q.reject(err);
            }

            var responseTransform = function responseTransform(result) {
              return _.map(result, function (value) {
                return { text: value };
              });
            };

            var metrics_regex = /metrics\((.*)\)/;
            var tag_names_regex = /tag_names\((.*)\)/;
            var tag_values_regex = /tag_values\((.*?),\s?(.*)\)/;
            var tag_names_suggest_regex = /suggest_tagk\((.*)\)/;
            var tag_values_suggest_regex = /suggest_tagv\((.*)\)/;

            var metrics_query = interpolated.match(metrics_regex);
            if (metrics_query) {
              return this._performSuggestQuery(metrics_query[1], 'metrics').then(responseTransform);
            }

            var tag_names_query = interpolated.match(tag_names_regex);
            if (tag_names_query) {
              return this._performMetricKeyLookup(tag_names_query[1]).then(responseTransform);
            }

            var tag_values_query = interpolated.match(tag_values_regex);
            if (tag_values_query) {
              return this._performMetricKeyValueLookup(tag_values_query[1], tag_values_query[2]).then(responseTransform);
            }

            var tag_names_suggest_query = interpolated.match(tag_names_suggest_regex);
            if (tag_names_suggest_query) {
              return this._performSuggestQuery(tag_names_suggest_query[1], 'tagk').then(responseTransform);
            }

            var tag_values_suggest_query = interpolated.match(tag_values_suggest_regex);
            if (tag_values_suggest_query) {
              return this._performSuggestQuery(tag_values_suggest_query[1], 'tagv').then(responseTransform);
            }

            return this.$q.when([]);
          }
        }, {
          key: 'performTimeSeriesQuery',
          value: function performTimeSeriesQuery(queries, start, end) {
            var msResolution = true;
            var responsePromises = [];

            // execute backend datasourceRequests separately
            // for better performance and load balancing
            _.each(queries, function (query) {

              var reqBody = {
                start: start,
                queries: [query],
                msResolution: msResolution,
                globalAnnotations: true,
                showQuery: true
              };

              // Relative queries (e.g. last hour) don't include an end time
              if (end) {
                reqBody.end = end;
              }

              var options = {
                method: 'POST',
                url: this.url + '/api/query?metric=' + query.metric,
                data: reqBody
              };
              this._addCredentialOptions(options);

              // In case the backend is 3rd-party hosted and does not suport OPTIONS, urlencoded requests
              // go as POST rather than OPTIONS+POST
              options.headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
              responsePromises.push(this.backendSrv.datasourceRequest(options));
            }.bind(this));

            // wait until all promised datasourceRequests complete
            // and then return combined responses
            return Promise.all(responsePromises).then(function (queryResponses) {
              var combinedResponse = queryResponses[0];
              for (var x = 1; x < queryResponses.length; x++) {
                combinedResponse.data = combinedResponse.data.concat(queryResponses[x].data);
              }
              return combinedResponse;
            }.bind(this));
          }
        }, {
          key: '_saveTagKeys',
          value: function _saveTagKeys(metricData) {
            var tagKeys = Object.keys(metricData.tags);
            _.each(metricData.aggregateTags, function (tag) {
              tagKeys.push(tag);
            });

            this.tagKeys[metricData.metric] = tagKeys;
          }
        }, {
          key: 'suggestTagKeys',
          value: function suggestTagKeys(metric) {
            return this.$q.when(this.tagKeys[metric] || []);
          }
        }, {
          key: '_performSuggestQuery',
          value: function _performSuggestQuery(query, type) {
            return this._get('/api/suggest', { type: type, q: query, max: 1000 }).then(function (result) {
              return result.data;
            });
          }
        }, {
          key: '_performMetricKeyValueLookup',
          value: function _performMetricKeyValueLookup(metric, keys) {

            if (!metric || !keys) {
              return $q.when([]);
            }

            var keysArray = keys.split(",").map(function (key) {
              return key.trim();
            });
            var key = keysArray[0];
            var keysQuery = key + "=.*";

            if (keysArray.length > 1) {
              keysQuery += "," + keysArray.splice(1).join(",");
            }

            var m = metric + "{" + keysQuery + "}";

            return this._get('/api/search/lookup', { m: m, limit: 3000 }).then(function (result) {
              result = result.data.results;
              var tagvs = [];
              _.each(result, function (r) {
                if (tagvs.indexOf(r.tags[key]) === -1) {
                  tagvs.push(r.tags[key]);
                }
              });
              return tagvs;
            });
          }
        }, {
          key: '_performMetricKeyLookup',
          value: function _performMetricKeyLookup(metric) {
            if (!metric) {
              return this.$q.when([]);
            }

            return this._get('/api/search/lookup', { m: metric, limit: 1000 }).then(function (result) {
              result = result.data.results;
              var tagks = [];
              _.each(result, function (r) {
                _.each(r.tags, function (tagv, tagk) {
                  if (tagks.indexOf(tagk) === -1) {
                    tagks.push(tagk);
                  }
                });
              });
              return tagks;
            });
          }
        }, {
          key: '_get',
          value: function _get(relativeUrl, params) {
            var options = {
              method: 'GET',
              url: this.url + relativeUrl,
              params: params
            };

            this._addCredentialOptions(options);

            return this.backendSrv.datasourceRequest(options);
          }
        }, {
          key: '_addCredentialOptions',
          value: function _addCredentialOptions(options) {
            if (this.basicAuth || this.withCredentials) {
              options.withCredentials = true;
            }
            if (this.basicAuth) {
              options.headers = { "Authorization": this.basicAuth };
            }
          }
        }, {
          key: 'getAggregators',
          value: function getAggregators() {
            if (this.aggregatorsPromise) {
              return this.aggregatorsPromise;
            }

            this.aggregatorsPromise = this._get('/api/aggregators').then(function (result) {
              if (result.data && _.isArray(result.data)) {
                return result.data.sort();
              }
              return [];
            });
            return this.aggregatorsPromise;
          }
        }, {
          key: 'getFilterTypes',
          value: function getFilterTypes() {
            if (this.filterTypesPromise) {
              return this.filterTypesPromise;
            }

            // filters are for a newer opentsdb api version than Timely originally copied.
            this.filterTypesPromise = this._get('/api/aggregators').then(function (result) {
              return [];
            });
            return this.filterTypesPromise;
          }
        }, {
          key: 'transformMetricData',
          value: function transformMetricData(md, groupByTags, target, options) {
            var metricLabel = this.createMetricLabel(md, target, groupByTags, options);
            var dps = [];

            // TSDB returns datapoints has a hash of ts => value.
            // Can't use _.pairs(invert()) because it stringifies keys/values
            _.each(md.dps, function (v, k) {
              dps.push([v, k * 1]);
            });

            return { target: metricLabel, datapoints: dps };
          }
        }, {
          key: 'createMetricLabel',
          value: function createMetricLabel(md, target, groupByTags, options) {
            if (target.alias) {
              var scopedVars = _.clone(options.scopedVars || {});
              _.each(md.tags, function (value, key) {
                scopedVars['tag_' + key] = { value: value };
              });
              return this.templateSrv.replace(target.alias, scopedVars);
            }

            var label = md.metric;
            var tagData = [];

            if (!_.isEmpty(md.tags)) {
              _.each(_.toPairs(md.tags), function (tag) {
                if (_.has(groupByTags, tag[0])) {
                  tagData.push(tag[0] + "=" + tag[1]);
                }
              });
            }

            if (!_.isEmpty(tagData)) {
              label += "{" + tagData.join(", ") + "}";
            }

            return label;
          }
        }, {
          key: 'convertTargetToQuery',
          value: function convertTargetToQuery(target, options) {
            if (!target.metric || target.hide) {
              return null;
            }

            var query = {
              metric: this.templateSrv.replace(target.metric, options.scopedVars),
              aggregator: "avg"
            };

            if (target.aggregator) {
              query.aggregator = this.templateSrv.replace(target.aggregator);
            }

            if (target.shouldComputeRate) {
              query.rate = true;
              query.rateOptions = {
                counter: !!target.isCounter
              };

              if (target.counterMax && target.counterMax.length) {
                query.rateOptions.counterMax = parseInt(target.counterMax);
              }

              if (target.counterResetValue && target.counterResetValue.length) {
                query.rateOptions.resetValue = parseInt(target.counterResetValue);
              }
            }

            if (!target.disableDownsampling) {
              var interval = this.templateSrv.replace(target.downsampleInterval || options.interval);

              if (interval.match(/\.[0-9]+s/)) {
                interval = parseFloat(interval) * 1000 + "ms";
              }

              query.downsample = interval + "-" + target.downsampleAggregator;

              if (target.downsampleFillPolicy && target.downsampleFillPolicy !== "none") {
                query.downsample += "-" + target.downsampleFillPolicy;
              }
            }

            if (target.filters && target.filters.length > 0) {
              query.filters = angular.copy(target.filters);
              if (query.filters) {
                for (var filter_key in query.filters) {
                  query.filters[filter_key].filter = this.templateSrv.replace(query.filters[filter_key].filter, options.scopedVars, 'pipe');
                }
              }
            } else {
              query.tags = angular.copy(target.tags);
              if (query.tags) {
                for (var tag_key in query.tags) {
                  query.tags[tag_key] = this.templateSrv.replace(query.tags[tag_key], options.scopedVars, 'pipe');
                }
              }
            }

            return query;
          }
        }, {
          key: 'mapMetricsToTargets',
          value: function mapMetricsToTargets(metrics, options) {
            var interpolatedTagValue;
            return _.map(metrics, function (metricData) {
              return _.findIndex(options.targets, function (target) {
                if (target.filters && target.filters.length > 0) {
                  return target.metric === metricData.metric;
                } else {
                  return target.metric === metricData.metric && _.every(target.tags, function (tagV, tagK) {
                    interpolatedTagValue = this.templateSrv.replace(tagV, options.scopedVars, 'pipe');
                    return metricData.tags[tagK] === interpolatedTagValue || interpolatedTagValue === "*";
                  }.bind(this));
                }
              }.bind(this));
            }.bind(this));
          }
        }, {
          key: 'convertToTSDBTime',
          value: function convertToTSDBTime(date, roundUp) {
            if (date === 'now') {
              return null;
            }

            date = dateMath.parse(date, roundUp);
            return date.valueOf();
          }
        }, {
          key: '_makeHttpsUrl',
          value: function _makeHttpsUrl() {
            return 'https://' + this.timelyHost + ':' + this.httpsPort;
          }
        }, {
          key: '_makeWsUrl',
          value: function _makeWsUrl() {
            return 'ws://' + this.timelyHost + ':' + this.wsPort;
          }
        }]);

        return TimelyDatasource;
      }());

      _export('TimelyDatasource', TimelyDatasource);
    }
  };
});
//# sourceMappingURL=datasource.js.map
