
import _ from 'lodash';
import {AuthsPageCtrl} from 'plugins/timely-app/components/authspage';

export class MetricsPageCtrl {

  constructor(backendSrv, datasourceSrv){
    this.hasDatasource = false;
    this.backendSrv = backendSrv;
    var apc = new AuthsPageCtrl(backendSrv, datasourceSrv);
    this.datasources = apc.getTimelyDatasources();
    if( this.datasources.length > 0){
      this.source = this.datasources[0];
      this.updateDatasource();
    }
  }

  updateDatasource(){
    this._clearMetrics();
    var uri = this._getURI(this.source.jsonData);
    this.backendSrv.request(
      { method: 'GET',
        url: uri,
        withCredentials: true,
        params: "" }
    ).then(
      response => {
        this.metrics = this._transformMetrics(response.metrics);
        this.hasDatasource = true;
      });
  }

  _clearMetrics(){
    this.metrics = [];
  }

  _transformMetrics(metrics){
    var ret = [];
    _.each(metrics, function(value, idx){
      var metric = {};
      metric.metric = value.metric;
      metric.tags = [];
      _.map(value.tags, function(tag, key){
//        result || (result = {});
//        result['name'] = tag.key;
//        result['values'] || (result['values'] = []);
//        result['values'].push(tag.value);
        var gIdx = _.findIndex(metric.tags, {'name': tag.key});
        var grouped;
        if( gIdx == -1 ){
          grouped = {name: '', values: []};
        } else {
          grouped = metric.tags[gIdx];
        }
        grouped.values.push(tag.value);
        grouped.values = _.sortBy(_.uniq(grouped.values))
        grouped.name = tag.key;
        if( gIdx > -1 ){
          metric.tags[gIdx] = grouped;
        } else {
          metric.tags.push(grouped);
        }
      }.bind(metric));
      metric.tags = _.sortBy(metric.tags, 'name')
      ret.push(metric);
    }.bind(ret));

    return _.sortBy(ret, 'metric');
  }

  _getURI(jsonData){
    return 'https://'+jsonData.timelyHost+':'+jsonData.httpsPort+'/api/metrics'
  }

}
MetricsPageCtrl.templateUrl = 'components/metrics.html';
