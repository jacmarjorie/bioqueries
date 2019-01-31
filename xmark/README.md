### XMark Shred Benchmark 

The data used here is generated from the [xmark data generator](https://projects.cwi.nl/xmark/downloads.html) via:

```
./xmlgen -f 10 -o test10.xml
```

`-f` is the scale factor. `-s` splits the data into even chunks (as done in PaxQuery paper).

XMark Query 8:

| Query  | Scale Factor | Query  | Shred  |
|---|---|---|---|
| Flat  | 1  | 1376 ms | NA |
| Flat  | 10  | 4933 ms  | NA  |
| Flat  | 25  | 11753 ms | NA  |
| Shred | 1  | 2348 ms  | 22ms  |
| Shred | 10  | 12925 ms  | 15 ms  |
| Shred | 25  | 36448 ms  | 15 ms  |
| Shred - Opt | 1  | 1471 ms  | 20 ms  |
| Shred - Opt | 10  | 4560 ms  | 17 ms  |
| Shred - Opt | 25  | 11956 ms  | 17 ms  |
