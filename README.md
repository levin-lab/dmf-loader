# dmf-loader
A Perl script to load monthly data updates from the Social Security Death Master File (DMF) into a local relational database. 

Use of this script assumes you have a license for access to the death master file. The DMF is provided by the National Technical Information Service (NTIS). See here http://www.ntis.gov/products/ssa-dmf/# for details on how to obtain access to the DMF.

ssndi-ddl.sql contains a create table statement to create a local table to use for storing the DMF. It is MySQL based but can easily be modified to work with any RDBMS.

load-ssndi-clean.pl is the actual load script. 
