import React from 'react';
import { QueryEditor } from './QueryEditor';
import {Icon, Tooltip} from '@grafana/ui';
import _ from 'lodash';
import { AsyncTypeahead } from 'react-bootstrap-typeahead';

export interface Props {
  tags: string[];
  parent: QueryEditor;
}

interface State {
  tags: DatasourceTag[];
  tagKeyOptionsLoading: boolean;
  tagKeyOptions: string[];
}

interface DatasourceTag {
  key: string;
  editing: boolean;
  errors: string[] | undefined;
}

export class DatasourceTagEditor extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      tags: [...this.props.tags]
        .map(key => ({
          key: key,
          editing: false,
          errors: undefined,
        }))
        .concat(),
      tagKeyOptionsLoading: false,
      tagKeyOptions: [],
    };
  }

  shouldComponentUpdate(): boolean {
    return true;
  }

  validateTags = (tags: DatasourceTag[], tagNumber: number): boolean => {
    let count = new Map<string, number>();
    let anyErrors = false;
    tags.forEach((tag) => {
      let n = count.get(tag.key);
      if (n === undefined) {
        count.set(tag.key, 1);
      } else {
        count.set(tag.key, n! + 1);
      }
    });

    let tagsToValidate: DatasourceTag[];
    if (tagNumber === undefined) {
      tagsToValidate = tags;
    } else {
      tagsToValidate = tags.filter((t, i) => i === tagNumber);
    }

    tagsToValidate.forEach((tag, i) => {
      tag.errors = [];
      if (tag.key.length > 0 && count.get(tag.key)! > 1) {
        tag.errors.push("duplicate tag key '" + tag.key + "'");
        anyErrors = true;
      }
      if (tag.key.length === 0) {
        tag.errors.push('tag key can not be empty');
        anyErrors = true;
      }
      if (tag.errors.length === 0) {
        tag.errors = undefined;
      }
    });

    return anyErrors;
  };

  addTag = () => {
    this.setState(state => {
      const stateTags = state.tags;
      let tags = [
        ...stateTags,
        {
          key: '',
          value: '',
          editing: true,
          errors: ['tag key can not be empty', 'tag value can not be empty'],
        },
      ];
      return { tags: tags };
    });
  };

  removeTag = (tagNumber: number) => {
    this.setState(state => {
      const tags = state.tags;
      tags.splice(tagNumber, 1);
      this.updateDatasourceTags(tags);
      return { tags: tags };
    });
  };

  beginEdit = (tagNumber: number) => {
    this.setState(state => {
      const tags = state.tags;
      let tag = tags.filter((t, i) => i === tagNumber).pop();
      if (tag !== undefined) {
        tag.editing = true;
      }
      return { tags: tags };
    });
  };

  endEdit = (tagNumber: number) => {
    this.setState(state => {
      const tags = state.tags;
      let tag = tags.filter((t, i) => i === tagNumber).pop();
      if (tag !== undefined && this.validateTags(tags, tagNumber) === false) {
        tag.editing = false;
        this.updateDatasourceTags(tags);
      }
      return { tags: tags };
    });
  };

  updateTagKey = (tagNumber: number, newKey: string) => {
    this.setState(state => {
      let tags = state.tags;
      let tag = tags.filter((t, i) => i === tagNumber).pop();
      if (tag !== undefined) {
        tag.key = newKey;
      }
      this.validateTags(tags, tagNumber);
      return { tags: tags };
    });
  };

  updateDatasourceTags = (tags: DatasourceTag[]) => {
    let datasourceTags: string[] = [ ]
    tags.forEach((tag) => {
      datasourceTags.push(tag.key);
    });
    this.props.parent.updateDatasourceTags(datasourceTags);
  };

  handleTagKeySearch = (query: string, metric: string | undefined) => {
    if (metric !== undefined) {
      this.props.parent.props.datasource
        .postResource('/api/suggest', {
          type: 'tagk',
          m: metric,
          max: '1000',
        })
        .then((result: any) => {
          if (result && _.isArray(result)) {
            const tagKeyArray: string[] = result;
            let tags = this.state.tags;

            // find all tag keys that are being used in non-edited tags
            let existingTags = tags
              .filter((t) => t.editing === false)
              .map((t) => {
                return t.key;
              })
              .concat();

            // exclude keys that are bing used
            let filteredResults = tagKeyArray.filter((t) => {
              return existingTags.find((existing) => t === existing) === undefined;
            });

            // exclude keys that don't match the search (if there is one)
            filteredResults = filteredResults
              .filter((tagKey) => query === '' || tagKey.toLowerCase().includes(query.toLowerCase()))
              .map((value) => {
                return value;
              });
            this.setState({ tagKeyOptions: filteredResults });
          }
        });
    }
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
    const { tagKeyOptionsLoading, tagKeyOptions } = this.state;
    let { tags } = this.state;
    let tagEditorId = Math.random().toString(36).substring(2, 9);
    let lastSearchMetric: string;
    const { metric } = this.props.parent.state.query;
    let allowAddNewTag = tags.filter((t) => t.editing === true).length === 0;

    return (
      <div className="gf-form-inline">
        <label className="gf-form-label width-12">Data source tags</label>
        {[...tags.values()].map((tag, tagNum) => (
          <div key={tagNum.toString()} id={'timelyDatasourceTag-' + tagNum.toString()}>
            <div hidden={tag.editing}>
              <label className="gf-form-label">
                {tag.key}
                <div className="timely tag edit">
                  <Tooltip content={'edit tag'} placement={'bottom'}>
                    <Icon
                      name="edit"
                      tabIndex={0}
                      onClickCapture={() => {
                        this.beginEdit(tagNum);
                      }}
                      onKeyDownCapture={(e) => {
                        if (e.key === 'Enter') {
                          this.beginEdit(tagNum);
                        }
                      }}
                    />
                  </Tooltip>
                </div>
                <div className="timely tag delete">
                  <Tooltip content={'delete tag'} placement={'bottom'}>
                    <Icon
                      name="trash-alt"
                      tabIndex={0}
                      onClickCapture={() => {
                        this.removeTag(tagNum);
                      }}
                      onKeyDownCapture={(e) => {
                        if (e.key === 'Enter') {
                          this.removeTag(tagNum);
                        }
                      }}
                    />
                  </Tooltip>
                </div>
              </label>
            </div>
            <div className="gf-form" hidden={!tag.editing}>
              <div
                id={'timelyDatasourceTagKey-' + tagEditorId + '-' + tagNum.toString()}
                className="timely timely-tag-key"
                onMouseLeave={(e) => {
                  let inputValue = this.getInputValue('timelyDatasourceTagKey-' + tagEditorId + '-' + tagNum.toString());
                  if (inputValue !== null) {
                    this.updateTagKey(tagNum, inputValue);
                  }
                }}
              >
                <AsyncTypeahead
                  id={'timelyDatasourceTagKey-' + tagEditorId + '-Typeahead'}
                  selected={[tag.key]}
                  isLoading={tagKeyOptionsLoading}
                  inputProps={{ spellCheck: false }}
                  onSearch={(query: string) => {
                    this.setState({ tagKeyOptionsLoading: true });
                    lastSearchMetric = metric!;
                    this.handleTagKeySearch(query, metric);
                    this.setState({ tagKeyOptionsLoading: false });
                  }}
                  onChange={() => {
                    let inputValue = this.getInputValue('timelyDatasourceTagKey-' + tagEditorId + '-' + tagNum.toString());
                    if (inputValue !== null) {
                      this.updateTagKey(tagNum, inputValue);
                    }
                  }}
                  onBlur={() => {
                    let inputValue = this.getInputValue('timelyDatasourceTagKey-' + tagEditorId + '-' + tagNum.toString());
                    if (inputValue !== null) {
                      this.updateTagKey(tagNum, inputValue);
                    }
                  }}
                  onFocus={() => {
                    let inputValue = this.getInputValue('timelyDatasourceTagKey-' + tagEditorId + '-' + tagNum.toString());
                    if (
                      tagKeyOptions.length === 0 ||
                      inputValue === null ||
                      inputValue.length === 0 ||
                      metric !== lastSearchMetric
                    ) {
                      this.setState({ tagKeyOptionsLoading: true });
                      lastSearchMetric = metric!;
                      this.handleTagKeySearch('', metric);
                      this.setState({ tagKeyOptionsLoading: false });
                    }
                  }}
                  onKeyDown={() => {
                    let inputValue = this.getInputValue('timelyDatasourceTagKey-' + tagEditorId + '-' + tagNum.toString());
                    if (
                      tagKeyOptions.length === 0 ||
                      inputValue === null ||
                      inputValue.length === 0 ||
                      metric !== lastSearchMetric
                    ) {
                      this.setState({ tagKeyOptionsLoading: true });
                      lastSearchMetric = metric!;
                      this.handleTagKeySearch('', metric);
                      this.setState({ tagKeyOptionsLoading: false });
                    }
                  }}
                  autoFocus={tag.key.length === 0}
                  options={tagKeyOptions}
                  selectHint={ (_shouldSelect, _e) => { return false; }}
                  align={'left'}
                  allowNew={true}
                  newSelectionPrefix={''}
                  paginate={true}
                  paginationText={'more'}
                  minLength={0}
                  maxResults={100}
                  caseSensitive={false}
                  placeholder={'key'}
                />
              </div>
              <label className="gf-form-label max-width-5 flex-shrink-1">
                {tag.errors !== undefined && (
                  <div className="timely tag error">
                    <Tooltip
                        content={
                        <React.Fragment>
                          <ul>
                            {tag.errors.map((error) => {
                              return <li key={error}>{error}</li>;
                            })}
                          </ul>
                        </React.Fragment>
                      }
                      placement={'bottom'}
                    >
                      <Icon name="exclamation-triangle" />
                    </Tooltip>
                  </div>
                )}
                {tag.errors === undefined && (
                  <div className="timely tag save">
                    <Tooltip content={'save tag'} placement={'bottom'}>
                      <Icon
                        name="save"
                        tabIndex={0}
                        onClickCapture={(e) => {
                          this.endEdit(tagNum);
                        }}
                        onKeyDownCapture={(e) => {
                          if (e.key === 'Enter') {
                            this.endEdit(tagNum);
                          }
                        }}
                      />
                    </Tooltip>
                  </div>
                )}
                <div className="timely tag delete">
                  <Tooltip content={'delete tag'} placement={'bottom'}>
                    <Icon
                      name="trash-alt"
                      tabIndex={0}
                      onClickCapture={(e) => {
                        this.removeTag(tagNum);
                      }}
                      onKeyDownCapture={(e) => {
                        if (e.key === 'Enter') {
                          this.removeTag(tagNum);
                        }
                      }}
                    />
                  </Tooltip>
                </div>
              </label>
            </div>
          </div>
        ))}
        <div className="gf-form" hidden={!allowAddNewTag}>
          <label className="gf-form-label">
            <div className="timely tag addtag">
              <Tooltip content={'add tag'} placement={'bottom'}>
                <Icon
                  name="plus"
                  tabIndex={0}
                  onClickCapture={(e) => {
                    this.addTag();
                  }}
                  onKeyDownCapture={(e) => {
                    if (e.key === 'Enter') {
                      this.addTag();
                    }
                  }}
                />
              </Tooltip>
            </div>
          </label>
        </div>
      </div>
    );
  }
}
