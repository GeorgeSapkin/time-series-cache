'use strict';

// 5 minutes
const DEFAULT_ALIGNMENT = 1000 * 60 * 5;

const DEFAULT_KEYS = {
  pageInfo:  '_page',
  timestamp: 'timestamp',
  value:     'value'
};

const DEFAULT_SORT = (a, b) => a > b;

const Slice = {
  Prev: [[-1]],
  Next: [[1, 2], [0, 1]]
};

function pageFrom(alignment, timestamp) {
  const value = timestamp.valueOf();
  return new Date(Math.floor(value / alignment) * alignment);
}

function pageTo(alignment, timestamp) {
  const value = timestamp.valueOf();
  // NB: not the same as Math.ceil, because ceil will be equal to floor if value
  //     is on the alignment boundary
  return new Date(Math.floor(value / alignment) * alignment + alignment);
}

function getPageRange(alignment, timestamp) {
  const from = pageFrom(alignment, timestamp);
  const to   = pageTo(alignment, timestamp);
  return { from, to };
}

function getValue(keys, from, to, slice, page) {
  const result = [...page.values]
    .sort(DEFAULT_SORT)
    .filter(([timestamp]) => from <= timestamp && timestamp < to)
    .slice(...slice)[0];

  if (result == null)
    return null;

  return {
    [keys.timestamp]: result[0],
    [keys.value]:     result[1],

    [keys.pageInfo]: {
      id:      page.id,
      version: page.version
    }
  };
}

function rangeToKey({ from }) {
  return from.valueOf();
}

function valuesToMap(keys, values) {
  return new Map(values.map(({
    [keys.timestamp]: timestamp, [keys.value]: value
  }) => [timestamp, value]));
}

class Page {
  constructor({ id, values, keys }) {
    this.id     = id;
    this.values = valuesToMap(keys, values);

    this.updated = new Date();
    this.version = 1;

    this._keys  = keys;
  }

  upsert(value) {
    const {
      [this._keys.timestamp]: timestamp,
      [this._keys.value]:     _value
    } = value;

    const existing = this.values.get(timestamp);
    if (existing === _value)
      return false;

    this.values.set(timestamp, _value);
    this.version++;
    this.updated = new Date();

    return true;
  }
}

class TimeseriesCache {
  constructor({
    loadPage,
    alignment = DEFAULT_ALIGNMENT,
    keys      = DEFAULT_KEYS
  }) {
    this._getPageRange = getPageRange.bind(null, alignment);
    this._keys         = keys;
    this._loadPage     = loadPage;

    this._pages     = new Map();
    this._pageLoads = 0;
  }

  get pageLoads() {
    return this._pageLoads;
  }

  get size() {
    return this._pages.size;
  }

  /**
   * Returns previous or next value within the date range.
   *
   * NB: `to - from <= alignment`
   *
   * @param {Date} from
   * @param {Date} to
   * @param {[number[]]} slice
   */
  async get(from, to, slice) {
    if (slice === Slice.Prev)
      return await this._getValue(from, to, slice[0], to) ||
        this._getValue(from, to, slice[0], from);

    return await this._getValue(from, to, slice[0], from) ||
      this._getValue(from, to, slice[1], to);
  }

  evictStalePages(since) {
    for (const [key, page] of this._pages) {
      if (since > page.updated)
        this._pages.delete(key);
    }
  }

  async isUpToDate(value) {
    const {
      [this._keys.timestamp]: timestamp,
      [this._keys.pageInfo]:  { id, version } = {}
    } = value;

    const range = this._getPageRange(timestamp);
    if (!this._hasPage(range))
      return false;

    const page = await this._getPage(range);

    return id === page.id && version === page.version;
  }

  async upsert(value) {
    const { [this._keys.timestamp]: timestamp } = value;

    const range = this._getPageRange(timestamp);
    const page  = await this._getPage(range);

    return page.upsert(value);
  }

  async _getPage(range) {
    const key = rangeToKey(range);
    if (this._pages.has(key))
      // Returns either a page stub that will be resolved later or a page.
      return this._pages.get(key);

    // Ensure page is only loaded once by creating a page stub that will be
    // resolved when page is loaded and will replace itself with the loaded
    // page.
    const pageStub = Promise.resolve()
      .then(() => this._loadPage(range))
      .then(values => {
        const page = new Page({
          id:   this._pageLoads++,
          keys: this._keys,
          values
        });
        this._pages.set(key, page);
        return page;
      });
    this._pages.set(key, pageStub);

    return pageStub;
  }

  async _getValue(from, to, slice, timestamp) {
    const range = this._getPageRange(timestamp);
    const page  = await this._getPage(range);

    return getValue(this._keys, from, to, slice, page);
  }

  _hasPage(range) {
    const key = rangeToKey(range);
    return this._pages.has(key);
  }
};

module.exports = {
  Slice,
  TimeseriesCache
};
