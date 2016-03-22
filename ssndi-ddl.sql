/***
* $Id: ssndi-ddl.sql 1375 2011-11-26 01:27:56Z patrick $
* Table to hold data from the social security death master file aka death index
* See https://dmf.ntis.gov/recordlayout.pdf
* Status is blank for full file, one of 'A', 'C', or 'D' for updates
* dob is 'date of birth'
* dod is 'date of death'
*
* As of 11/1/2011, state/country and zip info were removed for "privacy" reasons
*
* This sql can be used to create either the full table or the temp table for monthly updates
*/
DROP TABLE IF EXISTS death_index;
CREATE TABLE death_index (
    ssn         char(9) NOT NULL,
    last        varchar(20) NOT NULL,
    suffix      varchar(4) NULL,
    first       varchar(15) NOT NULL,
    middle      varchar(15) NULL,
    verified    char(1) NOT NULL,
    dodeath     date NOT NULL,
    dobirth     date NOT NULL,
    created     timestamp DEFAULT 0,
    updated     timestamp DEFAULT NOW() ON UPDATE NOW(),
    PRIMARY KEY (ssn),
    KEY ix_death_index_first(first),
    KEY ix_death_index_last(last),
    KEY ix_death_index_dobirth(dobirth)
)
ENGINE=InnoDB
PARTITION BY KEY("ssn")
PARTITIONS 127;



