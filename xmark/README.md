## XMark Shred Benchmark 

### Data Generation
The data used here is generated from the [xmark data generator](https://projects.cwi.nl/xmark/downloads.html) via:

### XMark Data Exploration

The XMark data generator has two flags that can be used to produce data:

```
 -f <factor>    scaling factor of the document, float value;
    0 produces the "minimal document"
    
 -s <cnt> split the doc in smaller chunks of only <cnt> elements each;
    useful for systems which cannot cope with large input 
    documents
```

Running the generator without `-s`, the data is a single top level record with nested bags (people, closed_auctions, etc). My thought was that using `-s` we could increase the level of sites (top level records). The one concerning thing about this is that `-s` does not guarantee that each site will actually have data inside the nested bag of people, closed_auctions, etc. 

* `-f` the scaling factor, increases the data size
* `-s` splits the data into a set of site files, which may contain some empty values for any of the bags (people, closed_auctions, etc.)
* `records` is the number of sites generated; sites = top level records
* `min size` is the smallest file size of all the files generated from `-s`
* `max size` is the largest file size of all the files generated from `-s`
* `total size (all files)` is the combined size of all files (this is what `-f` controls)
* `people and closed sizes` displays the distribution of people and closed_auction counts per site. This would allow us to explore effects of nested bag size; however, there are a lot of zeros.


| -f | -s | records | min size | max size | total size (all files) | people and closed sizes |
|---|---|---|---|---|---|---|
| 1 | 1000 | 70 | .5M | 3 M | 112 M | people: Map(450 -> 1, 1002 -> 25, 0 -> 44), closed: Map(576 -> 1, 156 -> 1, 1002 -> 9, 0 -> 59) |
| 1 | 2000 | 35 | .9M | 6 M | 112 M | people: Map(1476 -> 1, 2002 -> 12, 0 -> 22), closed: Map(1204 -> 1, 2002 -> 4, 538 -> 1, 0 -> 29) |
| 1 | 5000 | 15 | 2M | 13 M | 112 M | people: Map(490 -> 1, 5002 -> 5, 0 -> 9), closed: Map(5002 -> 1, 2232 -> 1, 2516 -> 1, 0 -> 12) |
| 1 | 10000 | 8 | 4M | 26 M | 112 M | people: Map(5496 -> 1, 10002 -> 2, 0 -> 5), closed: Map(7242 -> 1, 2508 -> 1, 0 -> 6) |
| 1 | 20000 | 5 | 6M | 51 M | 112 M | people: Map(5498 -> 1, 20002 -> 1, 0 -> 3), closed: Map(2504 -> 1, 7246 -> 1, 0 -> 3) | 
| 1 | 40000 | 3 | 13M | 57 M | 112 M | people: Map(25500 -> 1, 0 -> 2), closed: Map(7248 -> 1, 2502 -> 1, 0 -> 1) | 
| 10 | 1000 | 690 | .4M | 2.7M | 1.1G |  people: Map(492 -> 1, 1002 -> 254, 0 -> 435), closed: Map(750 -> 1, 558 -> 1, 1002 -> 96, 0 -> 592) |
| 10 | 2000 | 346 | .5M | 5M | 1.1G | people: Map(746 -> 1, 2002 -> 127, 0 -> 218), closed: Map(2002 -> 48, 28 -> 1, 1376 -> 1, 0 -> 296)|
| 10 | 5000 | 139 | 2M | 13M | 1.1G | people: Map(5002 -> 50, 4900 -> 1, 0 -> 88), closed: Map(5002 -> 19, 150 -> 1, 2312 -> 1, 0 -> 118)| 
| 10 | 10000 | 70 | 4M | 34M | 1.1G |people: Map(10002 -> 25, 4950 -> 1, 0 -> 44), closed: Map(10002 -> 9, 2406 -> 1, 5076 -> 1, 0 -> 59)| 
| 10 | 20000 | 35 | 8.5M | 60M | 1.1G | people: Map(14976 -> 1, 20002 -> 12, 0 -> 22), closed: Map(5038 -> 1, 12454 -> 1, 20002 -> 4, 0 -> 29)|
| 10 | 40000 | 18 | 18M | 102M | 1.1G | people: Map(40002 -> 6, 14988 -> 1, 0 -> 11), closed: Map(40002 -> 1, 25020 -> 1, 32478 -> 1, 0 -> 15) |


### Schema

The parser can be found at `src/main/scala/cs/ox/ac/uk/gdb/shred/XReader.scala`. Currently, the parser reads split files and treats each split as a site (this is what the table above is showing).

```
<site>
    <regions>
        <africa>
        <australia>
        ...
    </regions>
    <people>
        ...
    </people>
    <closed_auctions>
        ...
    </closed_auctions>
</site>
```

```
<people>
    <person id="person0">
        <name>Jaak Tempesti</name>
        <emailaddress>mailto:Tempesti@labs.com</emailaddress>
        <phone>+0 (873) 14873867</phone>
        <homepage>http://www.labs.com/~Tempesti</homepage>
        <creditcard>5048 5813 2703 8253</creditcard>
        <watches>
            <watch open_auction="open_auction0"/>
        </watches>
    </person>
    ...
```

```
<closed_auctions>
    <closed_auction>
        <seller person="person0"/>
        <buyer person="person0"/>
        <itemref item="item1"/>
        <price>113.87</price>
        <date>06/06/2000</date>
        <quantity>1</quantity>
        <type>Regular</type>
        <annotation>
        <author person="person0"/>
        ...
```

```
<regions>
    <africa>
        <item id="item0">
        <location>United States</location>
        <quantity>1</quantity>
        <name>duteous nine eighteen </name>
        <payment>Creditcard</payment>
        <description>
            <parlist>
            <listitem>
            <text>
            ...
    </africa>
    <europe>
    ...
```

### Queries

#### Query 8:

##### Original
```
for site1 in auction 
    for person in site1.people
        sng ( ( person.name, Mult( person.id, for site2 in auction 
                                for closed in site2.closed_auctions
                                    if closed.buyer = person.id
                                    then sng( person.id ) ) ) )   
```

##### Shredded:

DU is null.

```
NewLabel() -> for site1^flat in project1(auction^dict)(auction^flat)
                for person in project1(site1.people^dict)(site1.people^flat)
                    sng (person.name, Mult(person.id, For site2^flat in project1(auction^dict)(auction^flat) 
                                                        For closed^flat in project1(site2.closed^dict)(site2.closed^flat) 
                                                            if closed^flat.buyer = person^flat.id then sng(person^flat.id)))
```

##### Single site run

Scale factor is `-f` from data generator.

| Query  | Scale Factor | Query Time | Shred Time |
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

##### Multisite run

A quick look at multisite run for scale factors and various split sizes.

![TL](https://drive.google.com/uc?id=1yqE9S4zL-9hzsXGqCcl-jP4lbo8ypnIX)
![NB](https://drive.google.com/uc?id=14Qrhz4Z1CuPQyIFmYsYCm-GhMbhdpC-K)
