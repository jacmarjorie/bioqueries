### XMark Shred Benchmark 

#### Data Generation
The data used here is generated from the [xmark data generator](https://projects.cwi.nl/xmark/downloads.html) via:

```
./xmlgen -f 10 -o test10.xml
```

`-f` is the scale factor. `-s` splits the data into even chunks (as done in PaxQuery paper).

#### Data Schema

%md

## XMark Benchmark Synthetic Data

### Schema

The split option mentioned above will create XMLs with regions, people, closed_auctions, etc. split into even chunks. These splits represent data partitions in PaxQuery. The parser can be found at `src/main/scala/cs/ox/ac/uk/gdb/shred/XReader.scala`.

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

```
for site1 in auction 
    for person in site1.people
        sng ( ( person.name, Mult( person.id, for site2 in auction 
                                for closed in site2.closed_auctions
                                    if closed.buyer = person.id
                                    then sng( person.id ) ) ) )   
```

Shredded:

DU is null.

```
NewLabel() -> for site1^flat in project1(auction^dict)(auction^flat)
                for person in project1(site1.people^dict)(site1.people^flat)
                    sng (person.name, Mult(person.id, For site2^flat in project1(auction^dict)(auction^flat) 
                                                        For closed^flat in project1(site2.closed^dict)(site2.closed^flat) 
                                                            if closed^flat.buyer = person^flat.id then sng(person^flat.id)))
```
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
