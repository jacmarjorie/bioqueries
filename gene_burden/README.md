# Gene Burden Example

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
idenifier. Note that you main need to drop the information after the "." in gene name in order for these 
to match up to the values in the pathway information. The other value `ENSG` is known as an [Ensembl ID](http://www.ensembl.org/Homo_sapiens/Gene/Summary?db=core;g=ENSG00000010404;r=X:149476988-149521096). Though you may see these around, we will focus on using the gene name (`AL627309`) as a gene identifier.
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

### Using the Dataset API

Up to now, we have been using the RDD API. We find, in general, that there are performance benefits with using the Dataset API. 
At this point, you should extend your VCF loader function to return a Dataset type so you can describe your analyses in the 
Dataset API. You can see an example of how I have done this [here](https://github.com/jacmarjorie/bioqueries/blob/gwas/gwas/gwas/src/main/scala/gwas/VariantLoader.scala#L36-L45).

You shoud edit and extend the case class to include whatever information you feel necessary for your analysis, and these 
may correspond to various loader functions.

### Building cohorts with mutliple sample information

The VCF file in the previous section was a consensus VCF, meaning this is consolidated variant information for a group of 
samples (this is why the VCF file did not have sample information). We will now look at VCFs that contain multiple 
samples. Navigate to: `ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20130502/`, you will see a series of VCF files - one for each chromosome. Download `ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz`. This is one of the smaller chromsomes, but when you uncompress it, it will still be about 10G which is too large for local testing. Make a subset of this file by catting the first 10000 lines into a file: `head -n 10000 ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf > sub_chr22.vcf`.

**Note**: this is VCF4.1, you can see this in the header information of the VCF file (`head -1 sub_chr22.vcf`). 

If you look around at this file, you will see the INFO field is slightly different than the old one:
```
AC=1;AF=0.000199681;AN=5008;NS=2504;DP=8012;EAS_AF=0;AMR_AF=0;AFR_AF=0;EUR_AF=0;SAS_AF=0.001;AA=.|||;VT=SNP GT
```
This is just a list of metadata. If you are curious to know more about these fields, you can look at the `##INFO` lines in the header to see the description for these values. For example, `AC`:
```
##INFO=<ID=AC,Number=A,Type=Integer,Description="Total number of alternate alleles in called genotypes">
```
More importantly, you will also notice that these variants do not have annotations. It will be part of the task to associate these variants to genes by location. 

1. Download the map file at ftp://ftp.ensembl.org/pub/grch37/release-100/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.chr.gtf.gz. Since we are only using chr22 for now, you can subet this file to contain only chr22 genes. Read this into Spark into a Scala case class that would be suitable for accessing the necessary information. For example, to pair with the gene name we may want a case class with the type: `{(contig: Int, start: Int, end: Int, gene_name: String)}`. 

2. Add this table to your schema, and write out the NRC query that would correspond to joining this information with the variant information. Note that a gene has a contig (chromosome), start, and end position. A variant lies on a gene if the variant genomic position is on the same contig (chromosome) and lies within the inclusive range of the gene start and end positions. 

3. Perform this join in Spark using the Dataset API (ie. write the analysis corresponding to the NRC query you defined in 2.

4. Now that we have sample information write the NRC and the associated program to perform the following:
* extend the gene burden analysis to work for each sample, returning an nested object: 

`{(sample: String, genes: { (name: String, burden: Int) })}`

* extend the pathway burden analysis to work for each sample, play around with returning various types of output:

`{(sample: String, genes: { (name: String, burden: Int) })}`

and 

`{(sample: String, pathways: {(name: String, genes: { (name: String, burden: Int) })})}`

and

`{(sample: String, pathways: {(name: String, total_burden: Int, genes: { (name: String, burden: Int) })})}`

and

`{(sample: String, pathways: {(name: String, total_burden: Int)})}`

5. Now that we have by-sample information, use the above gene burden inputs to cluster patients based on their gene and pathway burden (write NRC and corresponding analysis for each). Start with a simple definition of gene burden - a count of 0 is no burden and anything over 0 is burdened:
* For each gene, create a group of samples that are burdened and not burdened: 

`{(gene_name: String, burdened: {(sample_name: String)}, not_burdened: {(sample_name: String)} )}`

* For each pathway, create a group of samples that are burdened and not burdened: 

`{(pathway_name: String, burdened: {(sample_name: String)}, not_burdened: {(sample_name: String)} )}`

6. Recall that we also have metadata information about the samples (mainly population information). Write the following NRC queries and perform the associated analysis to integrate this information:
* Group the variant information into samples by population: 

`{(population_name: String, samples: {(name: String, variants: {(...)})} )}`

* Calculate gene burden by population:

`{(gene: String, populations: {(name: String, burden: Int)} )}`

* Pathway by population: 

`{(pathway: String, genes: {(gene: String, populations: {(name: String, burden: Int)} )})}`

and 

`{(pathway: String, populations: {(name: String, burden: Int)} )}`

* Population by burden

`{(population: String, pathways: {(name: String, genes: { (name: String, burden: Int) } ) })}`

### Integrating the above into feature sets for a generic classification model 

**more information to come**: use what we have learned from the above queries to create categorical features as input to a generic classification model. For 1000 genomes we only have population information, so we use that as an example. The most basic thing being - 
can you use gene burden or pathway burden to determine the population from which the sample came? Basic counts aren't necessarily a good measure so we will work on upgrade this to a better function, integrating more annotation information / other biological data modalities, and also trying these methods with more interesting biomedical datasets (like TCGA) to maybe look at building feature sets for cancer datasets... 

### Useful Resources

* When you are working, you may want to subset the VCF file to make it only a few samples, 
you can do this with [bcftools](https://bioinformatics.stackexchange.com/questions/3477/how-to-subset-samples-from-a-vcf-file) for more information.

* If you run into issues using gene names (ie. they aren't unique identifiers), then you may find it useful to switch to using a more stable identifier. You can download the [pathway information with Entrez id's](https://www.gsea-msigdb.org/gsea/msigdb/download_file.jsp?filePath=/msigdb/release/7.1/c2.cp.v7.1.entrez.gmt). And you can use various mapping files to associate ids to eachother, such as those found at http://genome.ucsc.edu/cgi-bin/hgTables. It may take a bit of playing around to figure out your preferred method, but there are a lot of suggestions that come up on google.

