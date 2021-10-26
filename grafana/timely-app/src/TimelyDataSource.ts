import _ from 'lodash';
import {
  DataQuery,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceInstanceSettings,
  Field,
  Labels,
  MetricFindValue,
} from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { TimelyDataSourceOptions, TimelyQueryForm } from './types';
import { Observable } from 'rxjs';
import { HealthCheckResult } from '@grafana/runtime/utils/DataSourceWithBackend';
import { DataFrame } from '@grafana/data/types/dataFrame';
import { ScopedVars } from '@grafana/data/types/ScopedVars';
import { map } from 'rxjs/operators';

export class TimelyDataSource extends DataSourceWithBackend<TimelyQueryForm, TimelyDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<TimelyDataSourceOptions>) {
    super(instanceSettings);
  }

  /**
   * Ideally final -- any other implementation may not work as expected
   */
  query(request: DataQueryRequest<TimelyQueryForm>): Observable<DataQueryResponse> {
    // match query refId to TimelyQueryForm for correlation to results
    var targetIndex: { [key: string]: TimelyQueryForm } = {};
    request.targets.forEach((target) => {
      var query = target as TimelyQueryForm;
      targetIndex[query.refId] = query;
    });

    // use results at the alias in the corresponding TimelyQueryForm
    // to set the series name for display in the legend
    // there should be a better way to do this, but there isn't yet AFAIK
    return super.query(request).pipe(
      map((dataQueryResponse) => {
        dataQueryResponse.data.forEach((data) => {
          var dataFrame = data as DataFrame;
          var currentQuery = targetIndex[dataFrame.refId!];
          // prevent grafana from prepending the dataFrame name
          // to the series names by naming them all the same
          dataFrame.fields.forEach((field) => {
            this.setSeriesName(currentQuery, request.scopedVars, field, currentQuery.alias);
          });
        });
        return dataQueryResponse;
      })
    );
  }

  getSingleLabelName(labels: Labels): string | null {
    let singleName: string | null = null;
    for (const labelKey in labels) {
      if (singleName === null) {
        singleName = labelKey;
      } else if (labelKey !== singleName) {
        return null;
      }
    }
    return singleName;
  }

  joinLabels(labels: Labels, defaultValue = ''): string {
    if (!labels || Object.keys(labels).length === 0) {
      return defaultValue;
    }
    const labelKeys = Object.keys(labels).sort();
    const cleanSelector = labelKeys.map((key) => `${key}="${labels[key]}"`).join(', ');
    return ['{', cleanSelector, '}'].join('');
  }

  setSeriesName(query: TimelyQueryForm, scopedVars: ScopedVars, field: Field, alias: string | undefined) {
    var title: string | null = null;
    if (alias === undefined || alias === '') {
      if (field.labels === undefined) {
        title = query.metric!;
      } else {
        var singleTagName = this.getSingleLabelName(field.labels);
        if (singleTagName === null) {
          title = query.metric! + this.joinLabels(field.labels);
        } else {
          title = query.metric! + ' ' + field.labels[singleTagName];
        }
      }
    } else {
      var newScopedVars = _.clone(scopedVars || {});
      for (const labelKey in field.labels) {
        newScopedVars['tag_' + labelKey] = {
          text: field.labels[labelKey],
          value: field.labels[labelKey],
        };
      }
      scopedVars['metric'] = {
        text: query.metric,
        value: query.metric,
      };
      title = getTemplateSrv().replace(alias, newScopedVars);
    }
    field.config = {
      displayName: title,
    };
  }

  applyTemplateVariables(query: DataQuery): DataQuery {
    var timelyQuery = query as TimelyQueryForm;
    if (timelyQuery.metric !== undefined) {
      timelyQuery.metric = getTemplateSrv().replace(timelyQuery.metric);
      for (let tagsKey in timelyQuery.tags) {
        timelyQuery.tags[tagsKey] = getTemplateSrv().replace(timelyQuery.tags[tagsKey]);
      }
    }
    return timelyQuery;
  }

  getResource(path: string, params?: any): Promise<any> {
    return super.getResource(path, params);
  }

  postResource(path: string, body?: any): Promise<any> {
    return super.postResource(path, body);
  }

  _performSuggestQuery(params: object): Promise<string[]> {
    return this.postResource('/api/suggest', params).then(function (result) {
      return result;
    });
  }

  // DataSourceApi
  metricFindQuery(query: any, options?: any): Promise<MetricFindValue[]> {
    if (!query) {
      return new Promise<MetricFindValue[]>((resolve, reject) => {
        resolve([]);
      });
    }

    var interpolated;
    try {
      interpolated = getTemplateSrv().replace(query);
    } catch (err) {
      return new Promise<MetricFindValue[]>((resolve, reject) => {
        reject(err);
      });
    }

    var responseTransform = function (result: string[]): MetricFindValue[] {
      return _.map(result, function (value) {
        var mfv: MetricFindValue = {
          text: value,
        };
        return mfv;
      }).concat();
    };

    var metrics_regex = /metrics\((.*)\)/;
    var tag_names_suggest_regex = /suggest_tagk\((.*)\)/;
    var tag_values_suggest_regex = /suggest_tagv\((.*?),\s?(.*)\)/;
    var tag_names_regex = /tag_names\((.*)\)/;
    var tag_values_regex = /tag_values\((.*?),\s?(.*)\)/;

    var metrics_query = interpolated.match(metrics_regex);
    if (metrics_query) {
      return this._performSuggestQuery({
        type: 'metrics',
        m: metrics_query[1],
        max: '1000',
      }).then(responseTransform);
    }

    var tag_names_suggest_query = interpolated.match(tag_names_suggest_regex);
    if (tag_names_suggest_query) {
      return this._performSuggestQuery({
        type: 'tagk',
        m: tag_names_suggest_query[1],
        max: '1000',
      }).then(responseTransform);
    }

    var tag_values_suggest_query = interpolated.match(tag_values_suggest_regex);
    if (tag_values_suggest_query) {
      return this._performSuggestQuery({
        type: 'tagv',
        m: tag_values_suggest_query[1],
        t: tag_values_suggest_query[2],
        max: '1000',
      }).then(responseTransform);
    }

    var tag_names_query = interpolated.match(tag_names_regex);
    if (tag_names_query) {
      return this._performSuggestQuery({
        type: 'tagk',
        m: tag_names_query[1],
        max: '1000',
      }).then(responseTransform);
    }

    var tag_values_query = interpolated.match(tag_values_regex);
    if (tag_values_query) {
      return this._performSuggestQuery({
        type: 'tagv',
        m: tag_values_query[1],
        t: tag_values_query[2],
        max: '1000',
      }).then(responseTransform);
    }

    return new Promise<MetricFindValue[]>((resolve, reject) => {
      resolve([]);
    });
  }

  // DataSourceApi
  callHealthCheck(): Promise<HealthCheckResult> {
    return super.callHealthCheck();
  }

  // DataSourceApi
  testDatasource(): Promise<any> {
    return super.testDatasource();
  }

  // DataSourceApi
  getTagKeys(options?: any): Promise<MetricFindValue[]> {
    return this._performSuggestQuery({ type: 'tagk', max: '10000' }).then((result: any) => {
      if (result && _.isArray(result)) {
        const metrics: string[] = result;
        return metrics
          .map((value) => {
            var mfv: MetricFindValue = {
              text: value,
            };
            return mfv;
          })
          .concat();
      } else {
        return [];
      }
    });
  }

  // DataSourceApi
  getTagValues(options: any): Promise<MetricFindValue[]> {
    return this._performSuggestQuery({ type: 'tagv', max: '10000' }).then((result: any) => {
      if (result && _.isArray(result)) {
        const metrics: string[] = result;
        return metrics
          .map((value) => {
            var mfv: MetricFindValue = {
              text: value,
            };
            return mfv;
          })
          .concat();
      } else {
        return [];
      }
    });
  }
}
