import _ from 'lodash';
import { isValidDuration, QueryEditorProps, SelectableValue } from '@grafana/data';
import React from 'react';
import { TimelyDataSource } from '../TimelyDataSource';
import { TimelyDataSourceOptions, TimelyErrors, TimelyQueryForm } from '../types';
import { AsyncTypeahead } from 'react-bootstrap-typeahead';
import { TagEditor } from './TagEditor';

import {
  Icon,
  InlineField,
  InlineFieldRow,
  InlineSwitch,
  Input,
  Select,
} from '@grafana/ui';
import { DatasourceTagEditor } from './DatasourceTagEditor';

export type Props = QueryEditorProps<TimelyDataSource, TimelyQueryForm, TimelyDataSourceOptions>;

interface State {
  aggregatorTypes: string[];
  errors: TimelyErrors;
  query: TimelyQueryForm;
  queryOptionsLoading: boolean;
  queryOptions: string[];
}

export class QueryEditor extends React.Component<Props, State> {
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
      rateInterval: '1m',
      isCounter: false,
      tags: {},
      datasourceTags: []
    };
    this.state = {
      aggregatorTypes: ['none', 'avg', 'dev', 'max', 'min', 'sum', 'count'],
      errors: {
        downsample: undefined,
        rateInterval: undefined,
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
      let query = Object.assign(state.query, { metric: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onAliasChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { alias: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onAggregatorChange = (option: SelectableValue<string>) => {
    if (option.value) {
      const value = option.value;
      this.setState((state, props) => {
        let query = Object.assign(state.query, { aggregator: value });
        this.updateQuery(query);
        return { query: query };
      });
    }
  };

  onDownsampleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { downsampleInterval: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onDownsampleAggregatorChange = (option: SelectableValue<string>) => {
    if (option.value) {
      const value = option.value;
      this.setState((state, props) => {
        let query = Object.assign(state.query, { downsampleAggregator: value });
        this.updateQuery(query);
        return { query: query };
      });
    }
  };

  onDisableDownsamplingChange = (option: React.FormEvent<HTMLInputElement>) => {
    const checked = option.currentTarget.checked;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { disableDownsampling: checked });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onRateChange = (option: React.FormEvent<HTMLInputElement>) => {
    const checked = option.currentTarget.checked;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { shouldComputeRate: checked });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onRateIntervalChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { rateInterval: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onCounterChange = (option: React.FormEvent<HTMLInputElement>) => {
    const checked = option.currentTarget.checked;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { isCounter: checked });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onCounterMaxChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { counterMax: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  onCounterResetValueChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    this.setState((state, props) => {
      let query = Object.assign(state.query, { counterResetValue: value });
      this.updateQuery(query);
      return { query: query };
    });
  };

  updateQuery = (query: TimelyQueryForm) => {
    let isValid = this.validate()
    this.props.onChange(query);
    if (isValid) {
      this.props.onRunQuery();
    }
  };

  updateQueryFromState = () => {
    const query = this.state.query;
    this.props.onChange(query);
    this.props.onRunQuery();
  };

  validate = () => {
    const { disableDownsampling, downsampleInterval, shouldComputeRate, rateInterval } = this.state.query;
    let isValid = true
    let downsampleIntervalState = undefined;
    if (!disableDownsampling) {
      if (isValidDuration(downsampleInterval as string) === false) {
        downsampleIntervalState = "Invalid downsample interval (e.g. '120s', '1m', '1h')."
        isValid = false
      }
    }
    let rateIntervalState = undefined;
    if (shouldComputeRate && isValidDuration(rateInterval as string) === false) {
      rateIntervalState = "Invalid rate interval (e.g. '120s', '1m', '1h')."
      isValid = false
    }
    this.setState(
      {
        errors: {
          downsample: downsampleIntervalState,
          rateInterval: rateIntervalState
        },
      },
    );
    return isValid
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
      let query = Object.assign(state.query, { tags: tags });
      this.updateQuery(query);
      return { query: query };
    });
  };

  updateDatasourceTags = (datasourceTags: string[]) => {
    this.setState((state, props) => {
      let query = Object.assign(state.query, { datasourceTags: datasourceTags });
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
    let fullId = 'div#' + id + ' div div input.rbt-input-main';
    let inputElement = document.querySelector(fullId);
    if (inputElement !== null) {
      return (inputElement as HTMLInputElement).value;
    }
    return null;
  };

  render() {
    let queryEditorId = Math.random().toString(36).substring(2, 9);
    const { metric, aggregator, alias } = this.state.query;
    const { downsampleInterval, downsampleAggregator, disableDownsampling } = this.state.query;
    const { tags } = this.state.query;
    const { datasourceTags } = this.state.query;
    const { shouldComputeRate, rateInterval, isCounter, counterResetValue, counterMax } = this.state.query;
    const { errors } = this.state;
    const aggregatorOptions: Array<{}> = _.map(this.state.aggregatorTypes, (value: string) => ({
      label: value,
      value: value,
    }));
    const { queryOptionsLoading, queryOptions } = this.state;

    let multiplier = 7.5;
    let metricWidth = 300;
    if (metric !== undefined && metric!.length * multiplier > metricWidth) {
      metricWidth = Math.ceil(metric!.length * multiplier);
    }
    let aliasWidth = 175;
    if (alias !== undefined && alias!.length * multiplier > aliasWidth) {
      aliasWidth = Math.ceil(alias!.length * multiplier);
    }

      return (
        <div className="timely-edior-row">
          <InlineFieldRow>
            <InlineField label="Metric" labelWidth={24}>
              <div id={'timelyQuery-' + queryEditorId} className="timely timely-query">
                <AsyncTypeahead
                  id={'timelyQuery-' + queryEditorId + '-Typeahead'}
                  inputProps={{ spellCheck: false }}
                  defaultInputValue={metric}
                  isLoading={queryOptionsLoading}
                  onSearch={(query: string) => {
                    this.setState({ queryOptionsLoading: true });
                    this.handleSearch(query);
                    this.setState({ queryOptionsLoading: false });
                  }}
                  onChange={() => {
                    let inputValue = this.getInputValue('timelyQuery-' + queryEditorId);
                    if (inputValue !== null) {
                      this.onMetricTextChange(inputValue);
                    }
                  }}
                  onBlur={() => {
                    let inputValue = this.getInputValue('timelyQuery-' + queryEditorId);
                    if (inputValue !== null) {
                      this.onMetricTextChange(inputValue);
                    }
                  }}
                  onFocus={() => {
                    let inputValue = this.getInputValue('timelyQuery-' + queryEditorId);
                    if (queryOptions.length === 0 || inputValue === null || inputValue.length === 0) {
                      this.handleSearch('');
                    }
                  }}
                  options={queryOptions}
                  selectHint={(_shouldSelect, e) => {
                    return e.key === 'Enter';
                  }}
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
            </InlineField>
            <InlineField label="Alias" labelWidth={24} grow={true}>
              <Input
                id={'timelyAlias-' + queryEditorId}
                width={40}
                spellCheck={false}
                value={alias || ''}
                onChange={this.onAliasChange}
                placeholder={'series alias'}
              />
            </InlineField>
          </InlineFieldRow>
          <TagEditor tags={tags} parent={this} />
          <DatasourceTagEditor tags={datasourceTags} parent={this} />
          <InlineFieldRow>
            <InlineField label="Downsample" labelWidth={24}>
              <Input
                width={12}
                spellCheck={false}
                value={downsampleInterval || ''}
                onChange={this.onDownsampleChange}
                placeholder={'downsample interval'}
              />
            </InlineField>
            <InlineField label="Downsample aggregator" labelWidth={24}>
              <Select
                width={12}
                isSearchable={false}
                options={aggregatorOptions}
                value={downsampleAggregator}
                placeholder={'aggregator'}
                onChange={this.onDownsampleAggregatorChange}
                menuPlacement={'bottom'}
              />
            </InlineField>
            <InlineField label="Disable downsampling" labelWidth={24}>
              <InlineSwitch checked={disableDownsampling || false} onChange={this.onDisableDownsamplingChange} />
            </InlineField>
          </InlineFieldRow>
          <InlineFieldRow>
            <InlineField label="All results aggregator" labelWidth={24}>
              <Select
                width={12}
                isSearchable={false}
                options={aggregatorOptions}
                value={aggregator}
                placeholder={'aggregator'}
                onChange={this.onAggregatorChange}
                menuPlacement={'bottom'}
              />
            </InlineField>
            <InlineField label="Compute rate" labelWidth={24}>
              <InlineSwitch checked={shouldComputeRate || false} onChange={this.onRateChange} />
            </InlineField>
            <InlineField label="Rate interval" labelWidth={12} hidden={!shouldComputeRate}>
              <Input
                width={12}
                spellCheck={false}
                value={rateInterval || ''}
                onChange={this.onRateIntervalChange}
                placeholder={'rate interval'}
              />
            </InlineField>
            <InlineField label="Counter" labelWidth={12} hidden={!shouldComputeRate}>
              <InlineSwitch checked={isCounter || false} onChange={this.onCounterChange} />
            </InlineField>
            <InlineField label="Max Value" labelWidth={12} hidden={!shouldComputeRate || !isCounter}>
              <Input
                width={12}
                spellCheck={false}
                value={counterMax || ''}
                onChange={this.onCounterMaxChange}
                placeholder={'max value'}
              />
            </InlineField>
            <InlineField label="Reset Value" labelWidth={12} hidden={!shouldComputeRate || !isCounter}>
              <Input
                width={12}
                spellCheck={false}
                value={counterResetValue || ''}
                onChange={this.onCounterResetValueChange}
                placeholder={'reset value'}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow hidden={errors.downsample === undefined} className='queryErrorFieldRow'>
            <div className='queryErrorContainer'>
              <div className='queryErrorIconDiv'>
                <Icon name={'exclamation-triangle'} className='queryErrorIcon' />
              </div>
              <div className='queryErrorText'>{errors.downsample}</div>
            </div>
          </InlineFieldRow>
          <InlineFieldRow hidden={errors.rateInterval === undefined} className='queryErrorFieldRow'>
            <div className='queryErrorContainer'>
              <div className='queryErrorIconDiv'>
                <Icon name={'exclamation-triangle'} className='queryErrorIcon' />
              </div>
              <div className='queryErrorText'>{errors.rateInterval}</div>
            </div>
          </InlineFieldRow>
        </div>
      );
  }
}
