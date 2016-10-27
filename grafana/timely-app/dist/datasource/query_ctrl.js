'use strict';

System.register(['lodash', 'app/plugins/sdk'], function (_export, _context) {
  "use strict";

  var _, QueryCtrl, _createClass, TimelyQueryCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  function _possibleConstructorReturn(self, call) {
    if (!self) {
      throw new ReferenceError("this hasn't been initialised - super() hasn't been called");
    }

    return call && (typeof call === "object" || typeof call === "function") ? call : self;
  }

  function _inherits(subClass, superClass) {
    if (typeof superClass !== "function" && superClass !== null) {
      throw new TypeError("Super expression must either be null or a function, not " + typeof superClass);
    }

    subClass.prototype = Object.create(superClass && superClass.prototype, {
      constructor: {
        value: subClass,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
    if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass;
  }

  return {
    setters: [function (_lodash) {
      _ = _lodash.default;
    }, function (_appPluginsSdk) {
      QueryCtrl = _appPluginsSdk.QueryCtrl;
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

      _export('TimelyQueryCtrl', TimelyQueryCtrl = function (_QueryCtrl) {
        _inherits(TimelyQueryCtrl, _QueryCtrl);

        function TimelyQueryCtrl($scope, $injector, uiSegmentSrv) {
          _classCallCheck(this, TimelyQueryCtrl);

          var _this = _possibleConstructorReturn(this, (TimelyQueryCtrl.__proto__ || Object.getPrototypeOf(TimelyQueryCtrl)).call(this, $scope, $injector));

          _this.errors = _this.validateTarget();
          _this.aggregators = ['avg', 'sum', 'min', 'max', 'dev', 'zimsum', 'mimmin', 'mimmax'];
          _this.fillPolicies = ['none', 'nan', 'null', 'zero'];
          _this.filterTypes = ['wildcard', 'iliteral_or', 'not_iliteral_or', 'not_literal_or', 'iwildcard', 'literal_or', 'regexp'];

          if (!_this.target.aggregator) {
            _this.target.aggregator = 'sum';
          }

          if (!_this.target.downsampleAggregator) {
            _this.target.downsampleAggregator = 'avg';
          }

          if (!_this.target.downsampleFillPolicy) {
            _this.target.downsampleFillPolicy = 'none';
          }

          _this.datasource.getAggregators().then(function (aggs) {
            if (aggs.length !== 0) {
              _this.aggregators = aggs;
            }
          });

          _this.datasource.getFilterTypes().then(function (filterTypes) {
            if (filterTypes.length !== 0) {
              _this.filterTypes = filterTypes;
            }
          });

          // needs to be defined here as it is called from typeahead
          _this.suggestMetrics = function (query, callback) {
            _this.datasource.metricFindQuery('metrics(' + query + ')').then(_this.getTextValues).then(callback);
          };

          _this.suggestTagKeys = function (query, callback) {
            _this.datasource.suggestTagKeys(_this.target.metric).then(callback);
          };

          _this.suggestTagValues = function (query, callback) {
            _this.datasource.metricFindQuery('suggest_tagv(' + query + ')').then(_this.getTextValues).then(callback);
          };

          return _this;
        }

        _createClass(TimelyQueryCtrl, [{
          key: 'targetBlur',
          value: function targetBlur() {
            this.errors = this.validateTarget();
            this.refresh();
          }
        }, {
          key: 'getTextValues',
          value: function getTextValues(metricFindResult) {
            return _.map(metricFindResult, function (value) {
              return value.text;
            });
          }
        }, {
          key: 'addTag',
          value: function addTag() {

            if (this.target.filters && this.target.filters.length > 0) {
              this.errors.tags = "Please remove filters to use tags, tags and filters are mutually exclusive.";
            }

            if (!this.addTagMode) {
              this.addTagMode = true;
              return;
            }

            if (!this.target.tags) {
              this.target.tags = {};
            }

            this.errors = this.validateTarget();

            if (!this.errors.tags) {
              this.target.tags[this.target.currentTagKey] = this.target.currentTagValue;
              this.target.currentTagKey = '';
              this.target.currentTagValue = '';
              this.targetBlur();
            }

            this.addTagMode = false;
          }
        }, {
          key: 'removeTag',
          value: function removeTag(key) {
            delete this.target.tags[key];
            this.targetBlur();
          }
        }, {
          key: 'editTag',
          value: function editTag(key, value) {
            this.removeTag(key);
            this.target.currentTagKey = key;
            this.target.currentTagValue = value;
            this.addTag();
          }
        }, {
          key: 'closeAddTagMode',
          value: function closeAddTagMode() {
            this.addTagMode = false;
            return;
          }
        }, {
          key: 'addFilter',
          value: function addFilter() {

            if (this.target.tags && _.size(this.target.tags) > 0) {
              this.errors.filters = "Please remove tags to use filters, tags and filters are mutually exclusive.";
            }

            if (!this.addFilterMode) {
              this.addFilterMode = true;
              return;
            }

            if (!this.target.filters) {
              this.target.filters = [];
            }

            if (!this.target.currentFilterType) {
              this.target.currentFilterType = 'iliteral_or';
            }

            if (!this.target.currentFilterGroupBy) {
              this.target.currentFilterGroupBy = false;
            }

            this.errors = this.validateTarget();

            if (!this.errors.filters) {
              var currentFilter = {
                type: this.target.currentFilterType,
                tagk: this.target.currentFilterKey,
                filter: this.target.currentFilterValue,
                groupBy: this.target.currentFilterGroupBy
              };
              this.target.filters.push(currentFilter);
              this.target.currentFilterType = 'literal_or';
              this.target.currentFilterKey = '';
              this.target.currentFilterValue = '';
              this.target.currentFilterGroupBy = false;
              this.targetBlur();
            }

            this.addFilterMode = false;
          }
        }, {
          key: 'removeFilter',
          value: function removeFilter(index) {
            this.target.filters.splice(index, 1);
            this.targetBlur();
          }
        }, {
          key: 'editFilter',
          value: function editFilter(fil, index) {
            this.removeFilter(index);
            this.target.currentFilterKey = fil.tagk;
            this.target.currentFilterValue = fil.filter;
            this.target.currentFilterType = fil.type;
            this.target.currentFilterGroupBy = fil.groupBy;
            this.addFilter();
          }
        }, {
          key: 'closeAddFilterMode',
          value: function closeAddFilterMode() {
            this.addFilterMode = false;
            return;
          }
        }, {
          key: 'validateTarget',
          value: function validateTarget() {
            var errs = {};

            if (this.target.shouldDownsample) {
              try {
                if (this.target.downsampleInterval) {
                  kbn.describe_interval(this.target.downsampleInterval);
                } else {
                  errs.downsampleInterval = "You must supply a downsample interval (e.g. '1m' or '1h').";
                }
              } catch (err) {
                errs.downsampleInterval = err.message;
              }
            }

            if (this.target.tags && _.has(this.target.tags, this.target.currentTagKey)) {
              errs.tags = "Duplicate tag key '" + this.target.currentTagKey + "'.";
            }

            return errs;
          }
        }]);

        return TimelyQueryCtrl;
      }(QueryCtrl));

      _export('TimelyQueryCtrl', TimelyQueryCtrl);

      TimelyQueryCtrl.templateUrl = 'components/query.editor.html';
    }
  };
});
//# sourceMappingURL=query_ctrl.js.map
