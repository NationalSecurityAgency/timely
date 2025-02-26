import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { Icon, InlineField, InlineSwitch, Input, Tooltip } from '@grafana/ui';
import { TimelyDataSourceOptions } from '../types';
import * as React from 'react';

interface Props extends DataSourcePluginOptionsEditorProps<TimelyDataSourceOptions> {}

interface State {
  datasourceTags: DatasourceTag[];
}

interface DatasourceTag {
  key: string;
  value: string;
  index: number;
}

export class ConfigEditor extends React.PureComponent<Props, State> {
  constructor(props: Props) {
    super(props);
    let datasourceTags: DatasourceTag[] = [];
    let jsonDatasourceTags = props.options.jsonData.datasourceTags;
    if (jsonDatasourceTags && Object.entries(jsonDatasourceTags).length > 0) {
      let x = 0;
      datasourceTags = Object.entries(jsonDatasourceTags)
        .filter(([key, value]) => key !== "" && value !== "")
        .map(([key, value]) => ({
          key: key,
          value: value,
          index: x++
        }))
        .concat()
    }
    this.state = {
      datasourceTags: datasourceTags
    }
  }

  onTimelyHostChange = (event: React.FormEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      timelyHost: event.currentTarget.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onHttpsPortChange = (event: React.FormEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      httpsPort: parseInt(event.currentTarget.value, 10),
    };
    onOptionsChange({ ...options, jsonData });
  };

  onUseOAuthChange = (event: React.FormEvent<HTMLInputElement>) => {
    console.log(`onUseOAuthChang ${event.currentTarget.checked}`);
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      oauthPassThru: event.currentTarget.checked,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onUseClientCertWhenOAuthMissingChange = (event: React.FormEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      useClientCertWhenOAuthMissing: event.currentTarget.checked,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onClientCertificatePathChange = (event: React.FormEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      clientCertificatePath: event.currentTarget.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onClientKeyPathChange = (event: React.FormEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      clientKeyPath: event.currentTarget.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onCertificateAuthorityPathChange = (event: React.FormEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      certificateAuthorityPath: event.currentTarget.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onAllowInsecureSslChange = (event: React.FormEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      allowInsecureSsl: event.currentTarget.checked,
    };
    onOptionsChange({ ...options, jsonData });
  };

  addDatasourceTag = () => {
    const { onOptionsChange, options } = this.props;
    let jsonDatasourceTags = options.jsonData.datasourceTags;
    let datasourceTags = []
    jsonDatasourceTags = { ...jsonDatasourceTags, '': '' };
    datasourceTags = this.state.datasourceTags;
    datasourceTags.push({ key: "", value: "", index: 0})
    this.setState({ datasourceTags: datasourceTags });

    const jsonData = {
      ...options.jsonData,
      datasourceTags: jsonDatasourceTags,
    };
    onOptionsChange({ ...options, jsonData });
  };

  removeDatasourceTag = (index: number) => {
    const { onOptionsChange, options } = this.props;
    let jsonDatasourceTags = Object.assign({});
    let datasourceTags = []

    for (let x = 0; x < this.state.datasourceTags.length; x++) {
      let t = this.state.datasourceTags[x];
      if (x !== index) {
        jsonDatasourceTags = Object.assign({ ...jsonDatasourceTags, [t.key]: t.value });
        datasourceTags.push(t);
      }
    }

    this.setState({ datasourceTags: datasourceTags });
    const jsonData = {
      ...options.jsonData,
      datasourceTags: jsonDatasourceTags,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onChangeDatasourceTag(index: number, key: string, value: string) {
    const { onOptionsChange, options } = this.props;
    let jsonDatasourceTags = Object.assign({});
    let datasourceTags = []

    for (let x = 0; x < this.state.datasourceTags.length; x++) {
      let t = this.state.datasourceTags[x];
      if (x === index) {
        jsonDatasourceTags = Object.assign({ ...jsonDatasourceTags, [key]: value });
        datasourceTags.push({ key: key, value: value, index: index });
      } else {
        jsonDatasourceTags = Object.assign({ ...jsonDatasourceTags, [t.key]: t.value });
        datasourceTags.push(t);
      }
    }

    this.setState({ datasourceTags: datasourceTags });
    const jsonData = {
      ...options.jsonData,
      datasourceTags: jsonDatasourceTags,
    };
    onOptionsChange({ ...options, jsonData });
  }

  render() {
    const { options } = this.props;
    const { jsonData } = options;
    const { datasourceTags } = this.state;

    return (
      <div>
        <div className="gf-form-group">
          <div className="gf-form">
            <InlineField label={"Hostname or IP"} labelWidth={40}>
              <Input onChange={this.onTimelyHostChange} value={jsonData.timelyHost} width={100} />
            </InlineField>
          </div>
          <div className="gf-form">
            <InlineField label={"Https port"} labelWidth={40}>
              <Input onChange={this.onHttpsPortChange} value={jsonData.httpsPort} width={100} />
            </InlineField>
          </div>
          <div className="gf-form">
            <InlineField label={"Use OAuth token if available"} labelWidth={40}>
              <InlineSwitch value={jsonData.oauthPassThru || false} onChange={this.onUseOAuthChange} />
            </InlineField>
          </div>
          <div className="gf-form">
            <InlineField label={"Use client cert for users if OAuth is missing"} labelWidth={40}>
              <InlineSwitch
                value={jsonData.useClientCertWhenOAuthMissing || false}
                onChange={this.onUseClientCertWhenOAuthMissingChange}
              />
            </InlineField>
          </div>
          <div className="gf-form">
            <InlineField label={"Client cert path"} labelWidth={40}>
              <Input
                onChange={this.onClientCertificatePathChange}
                value={jsonData.clientCertificatePath || ''}
                width={100}
              />
            </InlineField>
          </div>
          <div className="gf-form">
            <InlineField label={"Client key path"} labelWidth={40}>
              <Input onChange={this.onClientKeyPathChange} value={jsonData.clientKeyPath || ''} width={100} />
            </InlineField>
          </div>
          <div className="gf-form">
            <InlineField label={"CA path"} labelWidth={40}>
              <Input
                onChange={this.onCertificateAuthorityPathChange}
                value={jsonData.certificateAuthorityPath || ''}
                width={100}
              />
            </InlineField>
          </div>
          <div className="gf-form">
            <InlineField label={"Allow insecure ssl"} labelWidth={40}>
              <InlineSwitch value={jsonData.allowInsecureSsl || false} onChange={this.onAllowInsecureSslChange} />
            </InlineField>
          </div>
        </div>
        <div className="gf-form-group">

          <InlineField label={"Data source tags"} labelWidth={20}>
            <div className="gf-form" hidden={datasourceTags.length > 0}>
              <label className="gf-form-label">
                <div className="timely tag addtag">
                  <Tooltip content={"add data source tag"} placement={"bottom"}>
                    <Icon
                      name="plus"
                      tabIndex={0}
                      onClickCapture={() => {
                        this.addDatasourceTag();
                      }}
                      onKeyDownCapture={(e) => {
                        if (e.key === 'Enter') {
                          this.addDatasourceTag();
                        }
                      }}
                    />
                  </Tooltip>
                </div>
              </label>
            </div>
          </InlineField>

          {[...datasourceTags.values()].map((tag, tagNumber) => (
            <div key={'dsTag-' + tagNumber} className="gf-form">
              <InlineField label={"Key"} labelWidth={20}>
                <Input
                  id={'datasourceTagKey-' + tagNumber}
                  width={20}
                  spellCheck={false}
                  value={tag.key}
                  onChange={(e) => this.onChangeDatasourceTag(tagNumber, e.currentTarget.value, tag.value)}
                />
              </InlineField>
              <InlineField label={"Value"} labelWidth={20}>
                <Input
                  id={'datasourceTagValue-' + tagNumber}
                  width={20}
                  spellCheck={false}
                  value={tag.value}
                  onChange={(e) => this.onChangeDatasourceTag(tagNumber, tag.key, e.currentTarget.value)}
                />
              </InlineField>
              <div className="gf-form">
                <label className="gf-form-label">
                  <div className="timely tag deletetag">
                    <Tooltip content={"delete data source tag"} placement={"bottom"}>
                      <Icon
                        name="minus"
                        tabIndex={0}
                        onClickCapture={() => {
                          this.removeDatasourceTag(tagNumber);
                        }}
                        onKeyDownCapture={(e) => {
                          if (e.key === 'Enter') {
                            this.removeDatasourceTag(tagNumber);
                          }
                        }}
                      />
                    </Tooltip>
                  </div>
                </label>
              </div>
              <div className="gf-form" hidden={tagNumber + 1 < datasourceTags.length}>
                <label className="gf-form-label">
                  <div className="timely tag addtag">
                    <Tooltip content={"add data source tag"} placement={"bottom"}>
                      <Icon
                        name="plus"
                        tabIndex={0}
                        onClickCapture={() => {
                          this.addDatasourceTag();
                        }}
                        onKeyDownCapture={(e) => {
                          if (e.key === 'Enter') {
                            this.addDatasourceTag();
                          }
                        }}
                      />
                    </Tooltip>
                  </div>
                </label>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }
}
