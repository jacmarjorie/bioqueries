#!/bin/bash

# translates the iRIGS analysis inputs to csv with "|" delimiters

# get files 
wget https://www.dropbox.com/sh/ubueim70ead34la/AAC4G00sHqXtv_dyPnfdU37xa/iRIGS_code.tar.gz?dl=0
tar -xzvf iRIGS_code.tar.gz\?dl\=0
cd iRIGS_code

# Snp
cat SNP_file/SCZ_108_loci | tr "\t" "|" | tail -n +2 > SNP_file/SCZ_108_loci.csv

# Gene
cat supporting_files/All_human_genes | tr "\t" "|" | tail -n +2 | sed 's/\.[0-9]//' > supporting_files/All_human_genes.csv
# Cp
cat supporting_files/BrainHiC/S22_TSS_CP.txt | cut -f 1-7 | tr "\t" "|" | tail -n +2 > supporting_files/BrainHiC/S22_TSS_CP.csv
# Gz
cat supporting_files/BrainHiC/S23_TSS_GZ.txt | tr -d '\r' | cut -f 1-7 | tr "\t" "|"  | tail -n +2 > supporting_files/BrainHiC/S23_TSS_GZ.csv
# Cap
cat supporting_files/capHiC/GM12878_DRE_number | tr "\t" "|" | tail -n +2 > supporting_files/capHiC/GM12878_DRE_number.csv
# Fantom 
cat supporting_files/Fantom5/enhancer_tss_associations.bed.txt | tr "\t|:|;" "|" | tail -n +2 > supporting_files/Fantom5/enhancer_tss_associations.csv

