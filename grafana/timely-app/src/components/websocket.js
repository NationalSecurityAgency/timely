
import {AuthsPageCtrl} from 'plugins/timely-app/components/authspage'


export class WebsocketCtrl {

  constructor($scope, $timeout, backendSrv, datasourceSrv) {
    this.$scope = $scope;
    this.$timeout = $timeout;

    var actrl = new AuthsPageCtrl(backendSrv, datasourceSrv);

    var datasources = actrl.getTimelyDatasources();
    this.datasource = datasources[0].jsonData;

    this.websocketSupported = false;
    if( window.WebSocket ){
        this.websocketSupported = true;
    }
    this.metric_count = 0;
    this.metric = { 'metric': 'empty',
                    'tags':[],
                    'timestamp': 0,
                    'value': 0.0
                    };

    this.fetching = false;
  }

  start(metric){
    this.fetching = true;
    var addr = this._getWebsocketAddress();
    this.sock = new WebSocket(addr);
    this.sock.onmessage = this._handleMessage.bind(this);
    this.sock.onopen = this._handleOpen.bind(this);
    this.sock.onclose = this._handleClose.bind(this);
  }

  stop(metric) {
    this.fetching = false;
    this.sock.close();
  }

  _handleMessage(message){
    //console.log('handleMessage');
    var metric = JSON.parse(message.data);

    this.metric.metric = metric.metric;
    this.metric.timestamp = metric.timestamp;
    this.metric.value = metric.value;
    this.metric.tags = metric.tags;
    this.metric_count = this.metric_count+1;
    this.$scope.$digest();
    //if( this.metrics.length >= 16){
    //    this.metrics.shift();
    //}
    //this.$scope.$apply( function(){
    //        this.metrics.push(metric);
    //        this.metric_count = this.metric_count + 1;
    //        }.bind(this));
  }

  _handleOpen(){
    var create = {
      "operation" : "create",
      "subscriptionId" : "12345"
    }
    this.sock.send(JSON.stringify(create));

    var m1 = {
      "operation" : "add",
      "subscriptionId" : "12345",
      "metric" : "sys.cpu.user"
    }
    this.sock.send(JSON.stringify(m1));

//    var m2 = {
//      "operation" : "add",
//      "subscriptionId" : "12345",
//      "metric" : "timely.keys.metric.inserted"
//    }
//    this.sock.send(JSON.stringify(m2));
  }

  _handleClose(){
    console.log("Websocket Closed")
  }

  _getWebsocketAddress(){
    return "wss://" + this.datasource.timelyHost + ":" + this.datasource.wsPort + "/websocket";
  }

}
WebsocketCtrl.templateUrl = 'components/websocket.html';


// example metric message
//{
//  "metric" : "sys.cpu.user",
//  "timestamp":1469028728091,
//  "value":1.0,
//  "tags":
//  [
//    {
//      "key":"rack",
//      "value":"r1"
//    },{
//      "key":"tag3",
//      "value":"value3"
//    },{
//      "key":"tag4",
//      "value":"value4"
//    }
//  ],
//  "subscriptionId":"<unique id>"
//}
