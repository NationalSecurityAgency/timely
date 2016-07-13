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


    this.$timeout(function(){
      console.log('timeout complete')
      this.fetching.done = true;
      this.fetching.status = 'success';
      this.fetching.title = "Success";

    }.bind(this), 3000);

  }

  doPostLogin(uri, authBlob){
    console.log('post ' + uri + ' <- ' + JSON.stringify(authBlob));

    this.backendSrv.post(uri, authBlob).then(function(result){
      this.handleLoginResult(result);
    }.bind(this));
  }

  doGetLogin(uri){
    console.log('get ' + uri);

    this.backendSrv.get(uri).then( function(result){
      this.handleLoginResult(result);
    }.bind(this));
  }

  handleLoginResult(result){
    console.log("Login Result:")
    console.log(result);

  }

  _getURI(jsonData){
    return 'https://'+jsonData.timelyHost+':'+jsonData.httpsPort+'/login'
  }

  _showResults(){

  }
}
