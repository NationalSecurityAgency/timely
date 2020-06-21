import React, { Component } from "react";
import { QueryEditor } from "./QueryEditor";
import { withStyles } from "@material-ui/core/styles";
import { Tooltip } from "@material-ui/core";
import { Icon } from "@grafana/ui";
import _ from "lodash";
import { AsyncTypeahead, TypeaheadModel } from "react-bootstrap-typeahead";

export interface Props {
  tags: { [key: string]: string };
  parent: QueryEditor;
}

interface State {
  tags: Tag[];
  tagKeyOptionsLoading: boolean;
  tagKeyOptions: TypeaheadModel[];
  tagValueOptionsLoading: boolean;
  tagValueOptions: TypeaheadModel[];
}

interface Tag {
  key: string;
  value: string;
  editing: boolean;
  errors: string[] | undefined;
}

const HtmlTooltip = withStyles(theme => ({
  tooltip: {
    backgroundColor: "#f5f5f9",
    color: "rgba(0, 0, 0, 0.87)",
    maxWidth: 220,
    fontSize: theme.typography.pxToRem(12),
    border: "1px solid #dadde9"
  }
}))(Tooltip);

export class TagEditor extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      tags: Object.entries(props.tags)
        .map(([key, value]) => ({
          key: key,
          value: value,
          editing: false,
          errors: undefined
        }))
        .concat(),
      tagKeyOptionsLoading: false,
      tagKeyOptions: [],
      tagValueOptionsLoading: false,
      tagValueOptions: []
    };
  }

  shouldComponentUpdate(
    nextProps: Readonly<Props>,
    nextState: Readonly<State>,
    nextContext: any
  ): boolean {
    return true;
  }

  validateTags = (tags: Tag[], tagNumber: number): boolean => {
    var count = new Map<string, number>();
    var anyErrors = false;
    tags.forEach(tag => {
      var n = count.get(tag.key);
      if (n === undefined) {
        count.set(tag.key, 1);
      } else {
        count.set(tag.key, n! + 1);
      }
    });

    var tagsToValidate: Tag[];
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
        tag.errors.push("tag key can not be empty");
        anyErrors = true;
      }
      if (tag.value.length === 0) {
        tag.errors.push("tag value can not be empty");
        anyErrors = true;
      }
      if (tag.errors.length === 0) {
        tag.errors = undefined;
      }
    });

    return anyErrors;
  };

  addTag = () => {
    this.setState((state, props) => {
      const stateTags = state.tags;
      var tags = [
        ...stateTags,
        {
          key: "",
          value: "",
          editing: true,
          errors: ["tag key can not be empty", "tag value can not be empty"]
        }
      ];
      return { tags: tags };
    });
  };

  removeTag = (tagNumber: number) => {
    this.setState((state, props) => {
      const tags = state.tags;
      tags.splice(tagNumber, 1);
      this.updateTags(tags);
      return { tags: tags };
    });
  };

  beginEdit = (tagNumber: number) => {
    this.setState((state, props) => {
      const tags = state.tags;
      var tag = tags.filter((t, i) => i === tagNumber).pop();
      if (tag !== undefined) {
        tag.editing = true;
      }
      return { tags: tags };
    });
  };

  endEdit = (tagNumber: number) => {
    this.setState((state, props) => {
      const tags = state.tags;
      var tag = tags.filter((t, i) => i === tagNumber).pop();
      if (tag !== undefined && this.validateTags(tags, tagNumber) === false) {
        tag.editing = false;
        this.updateTags(tags);
      }
      return { tags: tags };
    });
  };

  updateTagKey = (tagNumber: number, newKey: string) => {
    this.setState((state, props) => {
      var tags = state.tags;
      var tag = tags.filter((t, i) => i === tagNumber).pop();
      if (tag !== undefined) {
        tag.key = newKey;
      }
      this.validateTags(tags, tagNumber);
      return { tags: tags };
    });
  };

  updateTagValue = (tagNumber: number, newValue: string) => {
    this.setState((state, props) => {
      var tags = state.tags;
      var tag = tags.filter((t, i) => i === tagNumber).pop();
      if (tag !== undefined) {
        tag.value = newValue;
      }
      this.validateTags(tags, tagNumber);
      return { tags: tags };
    });
  };

  updateTags = (tags: Tag[]) => {
    var entries = Object.fromEntries(
      [...tags.values()].map(tag => [tag.key, tag.value]).concat()
    );
    this.props.parent.updateTags(entries);
  };

  handleTagKeySearch = (query: string, metric: string | undefined) => {
    if (metric !== undefined) {
      this.props.parent.props.datasource
        .postResource("/api/suggest", {
          type: "tagk",
          m: metric,
          max: "1000"
        })
        .then((result: any) => {
          if (result && _.isArray(result)) {
            const tagKeyArray: string[] = result;
            var tags = this.state.tags;

            // find all tag keys that are being used in non-edited tags
            var existingTags = tags
              .filter(t => t.editing === false)
              .map(t => {
                return t.key;
              })
              .concat();

            // exclude keys that are bing used
            var filteredResults = tagKeyArray.filter(t => {
              return (
                existingTags.find(existing => t === existing) === undefined
              );
            });

            // exclude keys that don't match the search (if there is one)
            filteredResults = filteredResults
              .filter(
                tagKey =>
                  query === "" ||
                  tagKey.toLowerCase().includes(query.toLowerCase())
              )
              .map(value => {
                return value;
              });
            this.setState({ tagKeyOptions: filteredResults });
          }
        });
    }
  };

  handleTagValueSearch = (
    query: string,
    metric: string | undefined,
    tagKey: string | undefined
  ) => {
    if (metric !== undefined && tagKey !== undefined) {
      this.props.parent.props.datasource
        .postResource("/api/suggest", {
          type: "tagv",
          m: metric,
          t: tagKey,
          max: "1000"
        })
        .then((result: any) => {
          if (result && _.isArray(result)) {
            const tagValueArray: string[] = [".*", ...result];
            const filteredResults = tagValueArray
              .filter(
                tagValue =>
                  query === "" ||
                  tagValue.toLowerCase().includes(query.toLowerCase())
              )
              .map(value => {
                return value;
              });
            this.setState({ tagValueOptions: filteredResults });
          }
        });
    }
  };

  getInputValue = (id: string): string | null => {
    var fullId = "div#" + id + " div div input.rbt-input-main";
    var inputElement = document.querySelector(fullId);
    if (inputElement !== null) {
      return (inputElement as HTMLInputElement).value;
    }
    return null;
  };

  render() {
    const {
      tagKeyOptionsLoading,
      tagKeyOptions,
      tagValueOptionsLoading,
      tagValueOptions
    } = this.state;
    var { tags } = this.state;
    var tagEditorId = Math.random()
      .toString(36)
      .substr(2, 9);
    var lastSearchMetric: string;
    const { metric } = this.props.parent.state.query;
    var allowAddNewTag = tags.filter(t => t.editing === true).length === 0;

    return (
      <div className="gf-form-inline">
        <div className="gf-form timely">
          <label className="gf-form-label width-10">Tags</label>
          {[...tags.values()].map((tag, tagNum) => (
            <div id={"timelytag-" + tagNum.toString()}>
              <div hidden={tag.editing}>
                <label className="gf-form-label">
                  {tag.key}&nbsp;=&nbsp;{tag.value}
                  <div className="timely tag edit">
                    <HtmlTooltip title={"edit tag"} placement={"bottom"}>
                      <Icon
                        name="edit"
                        tabIndex={0}
                        onClick={e => {
                          this.beginEdit(tagNum);
                        }}
                        onKeyPress={e => {
                          if (e.key === "Enter") {
                            this.beginEdit(tagNum);
                          }
                        }}
                      />
                    </HtmlTooltip>
                  </div>
                  <div className="timely tag delete">
                    <HtmlTooltip title={"delete tag"} placement={"bottom"}>
                      <Icon
                        name="trash-alt"
                        tabIndex={0}
                        onClick={e => {
                          this.removeTag(tagNum);
                        }}
                        onKeyPress={e => {
                          if (e.key === "Enter") {
                            this.removeTag(tagNum);
                          }
                        }}
                      />
                    </HtmlTooltip>
                  </div>
                </label>
              </div>
              <div className="gf-form" hidden={!tag.editing}>
                <div
                  id={"timelyTagKey-" + tagEditorId + "-" + tagNum.toString()}
                  className="timely timely-tag-key"
                  onMouseLeave={e => {
                    var inputValue = this.getInputValue(
                      "timelyTagKey-" + tagEditorId + "-" + tagNum.toString()
                    );
                    if (inputValue !== null) {
                      this.updateTagKey(tagNum, inputValue);
                    }
                  }}
                >
                  <AsyncTypeahead
                    id={"timelyTagKey-" + tagEditorId + "-Typeahead"}
                    selected={[tag.key]}
                    isLoading={tagKeyOptionsLoading}
                    inputProps={{ spellCheck: false }}
                    onSearch={query => {
                      this.setState({ tagKeyOptionsLoading: true });
                      lastSearchMetric = metric!;
                      this.handleTagKeySearch(query, metric);
                      this.setState({ tagKeyOptionsLoading: false });
                    }}
                    onChange={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagKey-" + tagEditorId + "-" + tagNum.toString()
                      );
                      if (inputValue !== null) {
                        this.updateTagKey(tagNum, inputValue);
                      }
                    }}
                    onBlur={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagKey-" + tagEditorId + "-" + tagNum.toString()
                      );
                      if (inputValue !== null) {
                        this.updateTagKey(tagNum, inputValue);
                      }
                    }}
                    onFocus={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagKey-" + tagEditorId + "-" + tagNum.toString()
                      );
                      if (
                        tagKeyOptions.length === 0 ||
                        inputValue === null ||
                        inputValue.length === 0 ||
                        metric !== lastSearchMetric
                      ) {
                        this.setState({ tagKeyOptionsLoading: true });
                        lastSearchMetric = metric!;
                        this.handleTagKeySearch("", metric);
                        this.setState({ tagKeyOptionsLoading: false });
                      }
                    }}
                    onKeyDown={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagKey-" + tagEditorId + "-" + tagNum.toString()
                      );
                      if (
                        tagKeyOptions.length === 0 ||
                        inputValue === null ||
                        inputValue.length === 0 ||
                        metric !== lastSearchMetric
                      ) {
                        this.setState({ tagKeyOptionsLoading: true });
                        lastSearchMetric = metric!;
                        this.handleTagKeySearch("", metric);
                        this.setState({ tagKeyOptionsLoading: false });
                      }
                    }}
                    autoFocus={tag.key.length === 0}
                    options={tagKeyOptions}
                    selectHintOnEnter={false}
                    align={"left"}
                    allowNew={true}
                    newSelectionPrefix={""}
                    bodyContainer={true}
                    paginate={true}
                    paginationText={"more"}
                    minLength={0}
                    maxResults={100}
                    caseSensitive={false}
                    placeholder={"key"}
                  />
                </div>
                <div
                  id={"timelyTagValue-" + tagEditorId + "-" + tagNum.toString()}
                  className="timely timely-tag-value"
                  onMouseLeave={e => {
                    var inputValue = this.getInputValue(
                      "timelyTagValue-" + tagEditorId + "-" + tagNum.toString()
                    );
                    if (inputValue !== null) {
                      this.updateTagValue(tagNum, inputValue);
                    }
                  }}
                >
                  <AsyncTypeahead
                    id={"timelyTagValue-" + tagEditorId + "-Typeahead"}
                    selected={[tag.value]}
                    isLoading={tagValueOptionsLoading}
                    inputProps={{ spellCheck: false }}
                    onSearch={query => {
                      this.setState({ tagValueOptionsLoading: true });
                      this.handleTagValueSearch(query, metric, tag.key);
                      this.setState({ tagValueOptionsLoading: false });
                    }}
                    onChange={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagValue-" +
                          tagEditorId +
                          "-" +
                          tagNum.toString()
                      );
                      if (inputValue !== null) {
                        this.updateTagValue(tagNum, inputValue);
                      }
                    }}
                    onBlur={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagValue-" +
                          tagEditorId +
                          "-" +
                          tagNum.toString()
                      );
                      if (inputValue !== null) {
                        this.updateTagValue(tagNum, inputValue);
                      }
                    }}
                    onFocus={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagValue-" +
                          tagEditorId +
                          "-" +
                          tagNum.toString()
                      );
                      if (
                        tagValueOptions.length === 0 ||
                        inputValue === null ||
                        inputValue.length === 0
                      ) {
                        this.setState({ tagValueOptionsLoading: true });
                        this.handleTagValueSearch("", metric, tag.key);
                        this.setState({ tagValueOptionsLoading: false });
                      }
                    }}
                    onKeyDown={e => {
                      var inputValue = this.getInputValue(
                        "timelyTagValue-" +
                          tagEditorId +
                          "-" +
                          tagNum.toString()
                      );
                      if (
                        tagValueOptions.length === 0 ||
                        inputValue === null ||
                        inputValue.length === 0
                      ) {
                        this.setState({ tagValueOptionsLoading: true });
                        this.handleTagValueSearch("", metric, tag.key);
                        this.setState({ tagValueOptionsLoading: false });
                      }
                    }}
                    options={tagValueOptions}
                    selectHintOnEnter={false}
                    align={"left"}
                    allowNew={true}
                    newSelectionPrefix={""}
                    bodyContainer={true}
                    paginate={true}
                    paginationText={"more"}
                    minLength={0}
                    maxResults={100}
                    caseSensitive={false}
                    placeholder={"value"}
                  />
                </div>
                <label className="gf-form-label max-width-5 flex-shrink-1">
                  {tag.errors !== undefined && (
                    <div className="timely tag error">
                      <HtmlTooltip
                        title={
                          <React.Fragment>
                            <ul>
                              {tag.errors.map(error => {
                                return <li>{error}</li>;
                              })}
                            </ul>
                          </React.Fragment>
                        }
                        placement={"bottom"}
                      >
                        <Icon name="exclamation-triangle" />
                      </HtmlTooltip>
                    </div>
                  )}
                  {tag.errors === undefined && (
                    <div className="timely tag save">
                      <HtmlTooltip title={"save tag"} placement={"bottom"}>
                        <Icon
                          name="save"
                          tabIndex={0}
                          onClick={e => {
                            this.endEdit(tagNum);
                          }}
                          onKeyPress={e => {
                            if (e.key === "Enter") {
                              this.endEdit(tagNum);
                            }
                          }}
                        />
                      </HtmlTooltip>
                    </div>
                  )}
                  <div className="timely tag delete">
                    <HtmlTooltip title={"delete tag"} placement={"bottom"}>
                      <Icon
                        name="trash-alt"
                        tabIndex={0}
                        onClick={e => {
                          this.removeTag(tagNum);
                        }}
                        onKeyPress={e => {
                          if (e.key === "Enter") {
                            this.removeTag(tagNum);
                          }
                        }}
                      />
                    </HtmlTooltip>
                  </div>
                </label>
              </div>
            </div>
          ))}
          <div className="gf-form" hidden={!allowAddNewTag}>
            <label className="gf-form-label">
              <div className="timely tag addtag">
                <HtmlTooltip title={"add tag"} placement={"bottom"}>
                  <Icon
                    name="plus"
                    tabIndex={0}
                    onClick={e => {
                      this.addTag();
                    }}
                    onKeyPress={e => {
                      if (e.key === "Enter") {
                        this.addTag();
                      }
                    }}
                  />
                </HtmlTooltip>
              </div>
            </label>
          </div>
        </div>
      </div>
    );
  }
}
