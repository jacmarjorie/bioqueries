## Gene Burden Example

Here we provide setup instructions for performing gene burden calculation in Spark. 

### Software

First download the required software:
* Scala (2.11)
* sbt
* [Spark](https://spark.apache.org/downloads.html), pre-built for Apache Hadoop 2.7.

### Data

Next download some example files:
* **Variants**: Example VCF, from 1000 genomes FTP site ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase1/analysis_results/functional_annotation/annotated_vcfs/ALL.wgs.integrated_phase1_release_v3_coding_annotation.20101123.snps_indels.sites.vcf.gz
* **Phenotype**: Basic phenotype information from 1000 genomes [here](https://www.internationalgenome.org/faq/can-i-get-phenotype-gender-and-family-relationship-information-samples/)
* **Pathways**: Gene pathway sets from [GSEA](https://www.gsea-msigdb.org/gsea/downloads.jsp). We want to to start with all canonical pathways, using gene symbols `c2.cp.v7.1.symbols.gmt`. You will need to make a login to access this file, you can do that [here](https://www.gsea-msigdb.org/gsea/register.jsp).

### Next Steps

The goal is to determine gene burden of various known biological pathways. More information on what this means [here](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3378813/). 

### Understanding Variant Information

To keep things simple let's work with a single VCF file. The goal is to calculate the gene burden 
for this VCF only. For simplicity, gene burden is just a raw count of the number of mutations 
on each gene.
  
Every line in the VCF file corresponds to a mutation, and a mutation will lie on a gene. 
This VCF file already has been tagged with the gene information - it is in the last column marked 
`INFO`. It will be useful to read a little bit about how a VCF works, details specific to 1000 genomes VCFs 
can be found [here](https://www.internationalgenome.org/wiki/Analysis/vcf4.0/). The first several lines 
marked with `##` are header lines. The VCF starts at the line marked with `#CHROM`. This is tab-delimited 
information. In the `INFO` column you can see `AL627309.2:ENSG00000249291.2`. This corresponds to a gene 
idenifier. The other value `ENSG` is known as an [Ensembl ID](http://www.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000010404;r=X:149476988-149521096). Though you may see these around, we will focus on using the gene name (`AL627309`) as a gene identifier.
```
#CHROM	POS	ID	      REF	ALT	QUAL	FILTER	INFO
1	13302	rs180734498	C	T	100	PASS	THETA=0.0048;AN=2184;AC=249;VT=SNP;AA=.;RSQ=0.6281;LDAF=0.1573;SNPSOURCE=LOWCOV;AVGPOST=0.8895;ERATE=0.0058;AF=0.11;ASN_AF=0.02;AMR_AF=0.08;AFR_AF=0.21;EUR_AF=0.14;VA=1:AL627309.2:ENSG00000249291.2:+:synonymous:1/1:AL627309.2-201:ENST00000515242.2:384_225_75_H->H
```
#### Subtasks

1. Think of the VCF as a table in a database. Using only the attributes important for the analysis, 
how would you describe this as a table in a database? What would the types of each column be?

2. Given the VCF input described in 1, write an NRC query that would represent this analysis.

3. Load this VCF into a Spark/Scala application and calculate the number of mutations that correspond to each gene. 

To help you get started with this, I have created a Spark/Scala starter package in the `burden` folder. Follow these instructions 
to get started:
* Clone this repo. 
* cd `bioqueries/gene_burden/burden`
* Open build.sbt and update with your scala and spark version  
* open `src/main/scala/burden/App.scala and change the basepath to the full path location of your VCF file.
* execute the application with `sbt run`, this will take a while the first time. Eventually it will execute and 
you will see variant information printed to the screen. Variants are loaded into VariantContext objects. This is 
part of the htsjdk api. You can find more information [here](https://samtools.github.io/htsjdk/javadoc/htsjdk/htsjdk/variant/variantcontext/VariantContext.html).
* running this through sbt is probably not the best way to proceed. The better way to run Spark applications is to build an application jar. This will require downloading the relevant dependency jars (as defined in the sbt file), updating your classpath, and then runnign the application jar with spark-submit. I'll leave this as an exercise for you to get familiar with setting up and executing spark applications.

4. What's similar about the NRC query from **2** and the analysis in **3**? What's different? 

#### Integrating more resources: pathway information

Now that we know how to work with a VCF file, let's make the query a bit more complicated. 
Calculate the gene burden for each pathway. Use the gene names in the pathway information file to 
assign each of the mutations in the VCF to a pathway.

1. Think of the VCF and Pathway information as tables in the same database. Draw a schema diagram 
using the VCF table you made above, add the pathway table, and show the dependency between the table.

2. Given the VCF and pathway inputs described in 1, write an NRC query that would represent the analyses 
described in **3** and **4** below.

3. Extend your analysis from above to calculate the total gene burden of each pathway. 

4. Alter this analysis to return a set of pathways with nested gene information, satisfying the following 
schema: 

`{(pathway: String, genes: {(name: String, burden: Int)})}`

5. Perform the analysis in Spark. How do these compare to the NRC queries, how are they different?

#### Building cohorts with mutliple sample information

This will include additional resources and dealing with multi-sample VCFs, such as those described [here](ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/). I will update these as we progress with the above.

### Useful Resources

When you are working, you may want to subset the VCF file to make it only a few samples, 
you can do this with [bcftools](https://bioinformatics.stackexchange.com/questions/3477/how-to-subset-samples-from-a-vcf-file) for more information.

