### MetaWear Python API

The MetaWear Python API is a (rather thin) wrapper around the C++ API. Thus, syntax is not particularly pythonic and function calls can be finnicky sometimes.

In particular, callback functions are generated as C objects inside a function call (not returned) but must be available in the all the scopes where it's called later. This is why callbacks are class attributes.

### Storing live-streamed data

Even though modern machines have a lot of RAM, it's not ideal to cache too much data in memory. Thus, the idea is to write data to disk as soon as possible and as quickly as possible.

Compressed data formats are nice to store statis data, but here the overhead of appending to a compressed file is an issue.

I compared writing to a SQLite database (no primary key) _versus_ writing to a plain csv text file:

    row = (1650667624.971, 0.013254)
    f = open('/tmp/test.csv', 'w', buffering=1)
    %timeit for _ in range(10000): f.write(f'{row[0]},{row[1]:.4f}')
    # 7.68 ms ± 69.7 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
    %timeit for _ in range(10000): db.execute("INSERT INTO timeseries VALUES(?, ?)", row)
    # 13.5 ms ± 109 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
    many_rows = (row for _ in range(10000))
    %timeit db.executemany("INSERT INTO timeseries VALUES(?, ?)", many_rows)
    # 538 ns ± 1.25 ns per loop (mean ± std. dev. of 7 runs, 1,000,000 loops each)

When inserting single rows, csv is the clear winner. However, for large chunsk of rows, SQLite is much faster. Keeping large chunks in memory, however, may lead to data loss. For now, I'll stick to csv files while streaming live data.
