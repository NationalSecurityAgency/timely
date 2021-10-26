import _ from 'lodash';
import { QueryEditorProps } from '@grafana/data';
import { Input, Select, LegacyForms } from '@grafana/ui';
import React, { Component } from 'react';
import { TimelyDataSource } from './TimelyDataSource';
import { TimelyDataSourceOptions, TimelyErrors, TimelyQueryForm } from './types';
import { SelectableValue } from '@grafana/data/types/select';
import dayjs from 'dayjs';
import { TagEditor } from './TagEditor';
import { AsyncTypeahead, TypeaheadModel } from 'react-bootstrap-typeahead';

const { Switch } = LegacyForms;

export type Props = QueryEditorProps<TimelyDataSource, TimelyQueryForm, TimelyDataSourceOptions>;

interface State {
  aggregatorTypes: string[];
  errors: TimelyErrors;
  query: TimelyQueryForm;
  queryOptionsLoading: boolean;
  queryOptions: TypeaheadModel[];
}

export class QueryEditor extends Component<Props, State> {
  metricArray: string[] = [];

  constructor(props: Props) {
    super(props);
    const defaultQuery: Partial<TimelyQueryForm> = {
      metric: '',
      alias: '',
      aggregator: 'none',
      refId: 'A',
      queryType: 'metricQuery',
      disableDownsampling: false,
      downsampleFillPolicy: 'none',
      downsampleInterval: '1m',
      downsampleAggregator: 'avg',
      shouldComputeRate: false,
      isCounter: false,
      tags: {},
    };
    this.state = {
      aggregatorTypes: ['none', 'avg', 'dev', 'max', 'min', 'sum', 'count'],
      errors: {
        downsample: undefined,
      },
      query: Object.assign({}, defaultQuery, props.query),
      queryOptionsLoading: false,
      queryOptions: [],
    };
    this.getAggregators();
    this.getMetrics();
  }

  shouldComponentUpdate(nextProps: Readonly<Props>, nextState: Readonly<State>, nextContext: any): boolean {
    return true;
  }

  onMetricTextChange = (value: string) => {
    this.setState((state, props) => {
      var query = Object.assign(state.query, { metric: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onAliasChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      var query = Object.assign(state.query, { alias: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onAggregatorChange = (option: SelectableValue<string>) => {
    if (option.value) {
      const value = option.value;
      this.setState((state, props) => {
        var query = Object.assign(state.query, { aggregator: value });
        this.updateQuery(query);
        return { query: query };
      });
    }
  };

  onDownsampleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      var query = Object.assign(state.query, { downsampleInterval: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onDownsampleAggregatorChange = (option: SelectableValue<string>) => {
    if (option.value) {
      const value = option.value;
      this.setState((state, props) => {
        var query = Object.assign(state.query, { downsampleAggregator: value });
        this.updateQuery(query);
        return { query: query };
      });
    }
  };

  onDisableDownsamplingChange = (option: React.FormEvent<HTMLInputElement>) => {
    const checked = option.currentTarget.checked;
    this.setState((state, props) => {
      var query = Object.assign(state.query, { disableDownsampling: checked });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onRateChange = (option: React.FormEvent<HTMLInputElement>) => {
    const checked = option.currentTarget.checked;
    this.setState((state, props) => {
      var query = Object.assign(state.query, { shouldComputeRate: checked });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onCounterChange = (option: React.FormEvent<HTMLInputElement>) => {
    const checked = option.currentTarget.checked;
    this.setState((state, props) => {
      var query = Object.assign(state.query, { isCounter: checked });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onCounterMaxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      var query = Object.assign(state.query, { counterMax: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onCounterResetValueChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      var query = Object.assign(state.query, { counterResetValue: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  updateQuery = (query: TimelyQueryForm) => {
    this.props.onChange(query);
    this.props.onRunQuery();
  };

  updateQueryFromState = () => {
    const query = this.state.query;
    this.props.onChange(query);
    this.props.onRunQuery();
  };

  validate = () => {
    const { disableDownsampling, downsampleInterval } = this.state.query;

    if (!disableDownsampling) {
      try {
        let valid = false;
        if (downsampleInterval) {
          valid = dayjs('', '').isValid();
        }
        if (!valid) {
          this.setState((state, props) => {
            this.setState(
              {
                errors: {
                  downsample: "You must supply a downsample interval (e.g. '1m' or '1h').",
                },
              },
              this.updateQueryFromState
            );
          });
        }
      } catch (err) {
        this.setState(
          {
            errors: {
              downsample: "You must supply a downsample interval (e.g. '1m' or '1h').",
            },
          },
          this.updateQueryFromState
        );
      }
    }
  };

  getAggregators = () => {
    this.props.datasource.getResource('/api/aggregators').then((result: any) => {
      if (result && _.isArray(result)) {
        this.setState({ aggregatorTypes: result }, this.updateQueryFromState);
      }
    });
  };

  getMetrics = () => {
    this.props.datasource.getResource('/api/suggest', { type: 'metrics', max: '-1' }).then((result: any) => {
      if (result && _.isArray(result)) {
        const metrics: string[] = result;
        this.metricArray = metrics
          .map((value) => {
            return value;
          })
          .concat();
      }
    });
  };

  updateTags = (tags: { [key: string]: string }) => {
    this.setState((state, props) => {
      var query = Object.assign(state.query, { tags: tags });
      this.updateQuery(query);
      return { query: query };
    });
  };

  handleSearch = (query: string) => {
    const results = this.metricArray
      .filter((metric) => query === '' || metric.toLowerCase().includes(query.toLowerCase()))
      .map((value) => {
        return value;
      });
    this.setState({ queryOptions: results });
  };

  getInputValue = (id: string): string | null => {
    var fullId = 'div#' + id + ' div div input.rbt-input-main';
    var inputElement = document.querySelector(fullId);
    if (inputElement !== null) {
      return (inputElement as HTMLInputElement).value;
    }
    return null;
  };

  render() {
    var queryEditorId = Math.random().toString(36).substr(2, 9);
    const { metric, aggregator, alias } = this.state.query;
    const { downsampleInterval, downsampleAggregator, disableDownsampling } = this.state.query;
    const { tags } = this.state.query;
    const { shouldComputeRate, isCounter, counterResetValue, counterMax } = this.state.query;
    const aggregatorOptions: Array<{}> = _.map(this.state.aggregatorTypes, (value: string) => ({
      label: value,
      value: value,
    }));
    const { queryOptionsLoading, queryOptions } = this.state;

    var multiplier = 7.5;
    var metricWidth = 300;
    if (metric !== undefined && metric!.length * multiplier > metricWidth) {
      metricWidth = Math.ceil(metric!.length * multiplier);
    }
    var aliasWidth = 175;
    if (alias !== undefined && alias!.length * multiplier > aliasWidth) {
      aliasWidth = Math.ceil(alias!.length * multiplier);
    }

    var dynamicStyle =
      'div.timely-query input.form-control, div#timelyQuery-' +
      queryEditorId +
      ' { ' +
      'width:' +
      metricWidth +
      'px!important ' +
      '}';

    dynamicStyle += '#timelyAlias-' + queryEditorId + ' { ' + 'width:' + aliasWidth + 'px!important ' + '}';

    return (
      <div className="timely-edior-row">
        <div className="gf-form-inline">
          <style>{dynamicStyle}</style>
          <div className="gf-form timely">
            <label className="gf-form-label width-10">Metric</label>
            <div id={'timelyQuery-' + queryEditorId} className="timely timely-query">
              <AsyncTypeahead
                id={'timelyQuery-' + queryEditorId + '-Typeahead'}
                inputProps={{ spellCheck: false }}
                defaultInputValue={metric}
                isLoading={queryOptionsLoading}
                onSearch={(query) => {
                  this.setState({ queryOptionsLoading: true });
                  this.handleSearch(query);
                  this.setState({ queryOptionsLoading: false });
                }}
                onChange={(e) => {
                  var inputValue = this.getInputValue('timelyQuery-' + queryEditorId);
                  if (inputValue !== null) {
                    this.onMetricTextChange(inputValue);
                  }
                }}
                onBlur={(e) => {
                  var inputValue = this.getInputValue('timelyQuery-' + queryEditorId);
                  if (inputValue !== null) {
                    this.onMetricTextChange(inputValue);
                  }
                }}
                onFocus={(e) => {
                  var inputValue = this.getInputValue('timelyQuery-' + queryEditorId);
                  if (queryOptions.length === 0 || inputValue === null || inputValue.length === 0) {
                    this.handleSearch('');
                  }
                }}
                options={queryOptions}
                selectHintOnEnter={true}
                align={'left'}
                allowNew={true}
                newSelectionPrefix={''}
                paginate={true}
                paginationText={'more'}
                minLength={0}
                maxResults={100}
                caseSensitive={false}
                placeholder={'metric'}
              />
            </div>
          </div>
          <div className="gf-form timely">
            <label className="gf-form-label width-5">Alias</label>
            <Input
              id={'timelyAlias-' + queryEditorId}
              className="min-width-10 flex-grow-1"
              spellCheck={false}
              value={alias || ''}
              onChange={this.onAliasChange}
              placeholder={'series alias'}
            />
          </div>
        </div>
        <TagEditor tags={tags} parent={this} />
        <div className="gf-form-inline">
          <div className="gf-form timely">
            <label className="gf-form-label width-10">Downsample</label>
            <Input
              className={'min-width-5 flex-grow-1'}
              spellCheck={false}
              value={downsampleInterval || ''}
              onChange={this.onDownsampleChange}
              placeholder={'interval'}
            />
          </div>
          <div className="gf-form timely">
            <label className="gf-form-label width-10">Downsample Aggregator</label>
            <Select
              className={'min-width-5 flex-grow-1'}
              isSearchable={false}
              options={aggregatorOptions}
              value={downsampleAggregator}
              placeholder={'aggregator'}
              onChange={this.onDownsampleAggregatorChange}
              menuPlacement={'bottom'}
            />
          </div>
          <div className="gf-form timely">
            <Switch
              label={'Disable downsampling'}
              labelClass="width-10"
              checked={disableDownsampling || false}
              onChange={this.onDisableDownsamplingChange}
            />
          </div>
        </div>
        <div className="gf-form-inline">
          <div className="gf-form timely">
            <label className="gf-form-label width-10">All Results Aggregator</label>
            <Select
              className={'min-width-5 flex-grow-1'}
              isSearchable={false}
              options={aggregatorOptions}
              value={aggregator}
              placeholder={'aggregator'}
              onChange={this.onAggregatorChange}
              menuPlacement={'bottom'}
            />
          </div>
          <div className="gf-form timely">
            <Switch
              label={'Rate'}
              labelClass="width-5"
              checked={shouldComputeRate || false}
              onChange={this.onRateChange}
            />
            <div hidden={!shouldComputeRate}>
              <Switch
                label={'Counter'}
                labelClass="width-10"
                checked={isCounter || false}
                onChange={this.onCounterChange}
              />
            </div>
            <div className="gf-form timely" hidden={!shouldComputeRate || !isCounter}>
              <label className="gf-form-label width-10">Max Value</label>
              <Input
                className={'min-width-6 flex-grow-1'}
                spellCheck={false}
                value={counterMax || ''}
                onChange={this.onCounterMaxChange}
                placeholder={'max value'}
              />
              <label className="gf-form-label width-10">Reset Value</label>
              <Input
                className={'min-width-6 flex-grow-1'}
                spellCheck={false}
                value={counterResetValue || ''}
                onChange={this.onCounterResetValueChange}
                placeholder={'reset value'}
              />
            </div>
          </div>
        </div>
      </div>
    );
  }
}
