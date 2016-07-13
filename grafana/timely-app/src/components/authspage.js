
import _ from 'lodash';
import {coreModule, appEvents} from 'app/core/core';
import {AuthsCtrl} from 'plugins/timely-app/components/auths';

export class AuthsPageCtrl {

  constructor(backendSrv, datasourceSrv){
    this.datasourceSrv = datasourceSrv;
    this.datasources = this.getTimelyDatasources();
    coreModule.controller('AuthsCtrl', AuthsCtrl);
  }

  getTimelyDatasources(){
    var allSources = this.datasourceSrv.getAll();

    var sources = _.map(allSources, function(value, key, col){
      var ret = {};
      ret.meta = {};
      ret.meta.type = col[key].meta.type;
      ret.meta.id = col[key].meta.id;
      ret.name = key;
      ret.jsonData = col[key].jsonData;
      return ret;
    });
    return _.filter(sources, function(d){
      return (d.meta.type === 'datasource' && d.meta.id === 'timely-datasource');
    });
  }

}
AuthsPageCtrl.templateUrl = 'components/auths.html';
