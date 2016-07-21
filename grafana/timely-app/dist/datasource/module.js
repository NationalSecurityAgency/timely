'use strict';

System.register(['./datasource', './query_ctrl', './config'], function (_export, _context) {
  "use strict";

  var TimelyDatasource, TimelyQueryCtrl, DatasourceConfigCtrl, TimelyAnnotationsQueryCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_datasource) {
      TimelyDatasource = _datasource.TimelyDatasource;
    }, function (_query_ctrl) {
      TimelyQueryCtrl = _query_ctrl.TimelyQueryCtrl;
    }, function (_config) {
      DatasourceConfigCtrl = _config.DatasourceConfigCtrl;
    }],
    execute: function () {
      _export('AnnotationsQueryCtrl', TimelyAnnotationsQueryCtrl = function TimelyAnnotationsQueryCtrl() {
        _classCallCheck(this, TimelyAnnotationsQueryCtrl);
      });

      TimelyAnnotationsQueryCtrl.templateUrl = 'components/annotations.editor.html';

      _export('Datasource', TimelyDatasource);

      _export('QueryCtrl', TimelyQueryCtrl);

      _export('ConfigCtrl', DatasourceConfigCtrl);

      _export('AnnotationsQueryCtrl', TimelyAnnotationsQueryCtrl);
    }
  };
});
//# sourceMappingURL=module.js.map
