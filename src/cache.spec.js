'use strict';

const { promisify } = require('util');

const _setTimeout = promisify(setTimeout);

const { Slice, TimeseriesCache } = require('./cache');

const THIRTY_SEC = 1000 * 30;

function getCache({
  bias = undefined,
  keys = undefined
} = {}) {
  return new TimeseriesCache({
    bias,
    keys,
    loadPage({ from, to }) {
      const res = [];

      let cursor = from.valueOf();
      while (cursor < to) {
        res.push({
          [keys ? keys.timestamp : 'timestamp']: new Date(cursor),
          [keys ? keys.value : 'value']:         3 + Math.random() * 97
        });

        cursor += THIRTY_SEC;
      }

      return res;
    }
  });
}

describe('TimeseriesCache', () => {
  test('only loads 2 pages with multiple parallel gets', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:00:00Z');
    const to   = new Date('0000-01-01T00:05:00Z');

    const parallelLoads = [
      cache.get(from, to, Slice.Prev),
      cache.get(from, to, Slice.Prev),
      cache.get(from, to, Slice.Prev),
      cache.get(from, to, Slice.Prev),
      cache.get(from, to, Slice.Prev),
      cache.get(from, to, Slice.Prev),
      cache.get(from, to, Slice.Prev)
    ];

    await Promise.all(parallelLoads);
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);
  });

  test('works with custom bias', async() => {
    const cache = getCache({
      bias: 1000 * 37
    });

    const from = new Date('0000-01-01T00:00:37Z');
    const to  = new Date('0000-01-01T00:05:37Z');

    const value = await cache.get(from, to, Slice.Prev);
    expect(value).toMatchObject({
      value: expect.any(Number),
      _page: {
        id: 1,
        version: 1
      },
      timestamp: new Date(to - THIRTY_SEC)
    });
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);
  });

  test('works with custom keys', async() => {
    const cache = getCache({
      keys: {
        pageInfo:  'metadata',
        timestamp: 'time',
        value:     'data'
      }
    });

    const from = new Date('0000-01-01T00:00:00Z');
    const to  = new Date('0000-01-01T00:05:00Z');

    const value = await cache.get(from, to, Slice.Prev);
    expect(value).toMatchObject({
      data: expect.any(Number),
      metadata: {
        id: 1,
        version: 1
      },
      time: new Date(to - THIRTY_SEC)
    });
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);
  });

  test('previous value is 30 seconds before to in from page', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:00:00Z');
    const to  = new Date('0000-01-01T00:05:00Z');

    const value = await cache.get(from, to, Slice.Prev);
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);

    const { timestamp } = value;
    expect(timestamp).toEqual(new Date(to.valueOf() - THIRTY_SEC));
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);
  });

  test('previous value is 30 seconds before to in to page', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:00:30Z');
    const to  = new Date('0000-01-01T00:05:30Z');

    const value = await cache.get(from, to, Slice.Prev);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    const { timestamp } = value;
    expect(timestamp).toEqual(new Date(to.valueOf() - THIRTY_SEC));
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);
  });

  test('next value is 30 seconds after from in from page', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:05:00Z');
    const to  = new Date('0000-01-01T00:10:00Z');

    const value = await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    const { timestamp } = value;
    expect(timestamp).toEqual(new Date(from.valueOf() + THIRTY_SEC));
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);
  });

  test('next value is 30 seconds after from in to page', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:04:30Z');
    const to  = new Date('0000-01-01T00:09:30Z');

    const value = await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);

    const { timestamp } = value;
    expect(timestamp).toEqual(new Date(from.valueOf() + THIRTY_SEC));
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);
  });

  test('value is up to date', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:05:00Z');
    const to  = new Date('0000-01-01T00:10:00Z');

    const value = await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    expect(await cache.isUpToDate(value)).toBeTruthy();
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);
  });

  test('value is up to date after upsert of same value', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:05:00Z');
    const to  = new Date('0000-01-01T00:10:00Z');

    const value = await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    await cache.upsert(value);

    expect(await cache.isUpToDate(value)).toBeTruthy();
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);
  });

  test('value is up to date after upsert into different page', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:05:00Z');
    const to  = new Date('0000-01-01T00:10:00Z');

    const value = await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    await cache.upsert({
      timestamp: new Date('0000-01-01T00:01:53Z'),
      value: 50
    });
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);

    expect(await cache.isUpToDate(value)).toBeTruthy();
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);
  });

  test('value is outdated after upsert into same page', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:05:00Z');
    const to  = new Date('0000-01-01T00:10:00Z');

    const value = await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    await cache.upsert({
      timestamp: new Date('0000-01-01T00:05:37Z'),
      value: 50
    });
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    expect(await cache.isUpToDate(value)).toBeFalsy();
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);
  });

  test('value is outdated when the page is not cached', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:05:00Z');
    const to  = new Date('0000-01-01T00:10:00Z');

    await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    expect(await cache.isUpToDate({
      timestamp: new Date('0000-01-01T00:01:53Z'),
      value: 50
    })).toBeFalsy();

    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);
  });

  test('value is outdated after stale pages are evicted', async() => {
    const cache = getCache();

    const from = new Date('0000-01-01T00:00:00Z');
    const to  = new Date('0000-01-01T00:05:00Z');

    await cache.get(from, to, Slice.Next);
    expect(cache.pageLoads).toBe(1);
    expect(cache.size).toBe(1);

    await _setTimeout(20);

    const from1 = new Date('0000-01-01T00:05:00Z');
    const to1  = new Date('0000-01-01T00:10:00Z');

    await cache.get(from1, to1, Slice.Next);
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(2);

    cache.evictStalePages(new Date(Date.now() - 10));
    expect(cache.pageLoads).toBe(2);
    expect(cache.size).toBe(1);
  });
});
