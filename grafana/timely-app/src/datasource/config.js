import _ from 'lodash';


export class DatasourceConfigCtrl {

  constructor() {
    this.current.jsonData = this.current.jsonData || {};
    this.current.access = 'direct';
    this.current.basicAuth = false;
    this.current.withCredentials = true;
    this.current.suggestHost = 'localhost';
  }
}
DatasourceConfigCtrl.templateUrl = 'datasource/partials/config.html';
