import _ from 'lodash';

export class AuthsCtrl {

  constructor(backendSrv, $rootScope, $timeout){
    this.$rootScope = $rootScope;
    this.$timeout = $timeout;
    this.backendSrv = backendSrv;
  }

  login(datasource){
    this.fetching = {};
    this.fetching.done = false;
    this.fetching.show = true;
    this.fetching.status = 'warning';
    this.fetching.title = 'Fetching auths...';

    //this.$rootScope.appEvent('alert-success', ['Authorization Cookie Received', '']);
    //this.$rootScope.appEvent('alert-error', ['Authorization Failed', '']);
    //this.$rootScope.appEvent('alert-warning', ['Fetching Authorizations', '']);

    var uri = this._getURI(datasource.jsonData);

    if(datasource.jsonData.basicAuths){
      if( this.basicAuthUser && this.basicAuthPassword){
        var authBlob = {username: this.basicAuthUser, password: this.basicAuthPassword};
        this.doPostLogin(uri, authBlob);
      } else {
        this.$rootScope.appEvent('alert-warning', [datasource.name, 'requires User/Password']);
        this.fetching.status = 'warning';
        this.fetching.title = 'User/Password required';

        return;
      }
    } else {
      this.doGetLogin(uri);
    }

  }

  doPostLogin(uri, authBlob){
    this.doXHR('POST', uri, authBlob);
  }

  doGetLogin(uri){
    this.doXHR('GET', uri)
  }

  doXHR(verb, uri, data){
    if( !data){
      data = "";
    }
    var r = new XMLHttpRequest();
    r.open(verb, uri, true);
    r.withCredentials = true;
    r.setRequestHeader('Content-Type', 'application/json');
    r.onreadystatechange = function () {
      this.handleLoginResult(r);
    }.bind(this);
    r.send(JSON.stringify(data));
  }

  handleLoginResult(response){
    // wait for readyState to be '4 => done'
    if (response.readyState != 4) return;

    this.fetching.done = true;

    switch(response.status){
      case 200:
        console.log(this.getCookie('TSESSIONID'))

        this.fetching.status = 'success';
        this.fetching.title = 'Login successful';
        this.$rootScope.appEvent('alert-success', ['Success', '']);
        break;
      default:
        this.fetching.status = 'warning';
        this.fetching.title = response.statusText;
        this.$rootScope.appEvent('alert-warning', [response.status+': '+response.statusText,'']);
        break;
    }

    if (r.readyState != 4 || r.status != 200) return;
    alert("Success: " + r.getResponseHeader('Set-Cookie'));
  }

  getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    if (parts.length == 2) return parts.pop().split(";").shift();
  }

  _getURI(jsonData){
    return 'https://'+jsonData.timelyHost+':'+jsonData.httpsPort+'/login'
  }

  _showResults(){

  }
}
