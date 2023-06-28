# hismatch
R package to link historical individual-level data. Optimized to run on many cores. Read documentation [here](https://eirikberger.github.io/hismatch/) or below. 

## Installation

The development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("eirikberger/hismatch")
```

Use `auth_token` option to use personal access tokens while the repo is still privat.

## Usage

A brief example of how to setup the class and run it. 

``` r
library(data.table)
library(furrr)
library(hismatch)

dta1 <- fread("~/Dropbox/shared_server/deathDataFromFMB/processed_data/dar_1928-1945.csv")[1:1000]
dta2 <- copy(dta1)

plan(multisession, workers = 1L)

linkingTest <- Hismatch$new(data1 = dta1,
                            data2 = dta2, 
                            firstname='firstname', 
                            surname='lastname',
                            blocks=c(),
                            dist_thr=0.5, 
                            rel_thr=FALSE, 
                            max_block_size=50000, 
                            matching_method=c("jw"))

# define matching blocks
linkingTest$blocks <- c('l_first', 'l_sur', 'byear', 'bmonth', 'bday', 'male')

# execute fuzzy matching
linkingTest$runMatching()
```

## Inspect results

Examples of how to extract information and data based on fuzzy match, as well as to save and read results. 

``` r
# print basic matching statistics
linkingTest

# other information and data
linkingTest$getMatchingRatesByVariable_DT(variable_list="birth_year", data=linkingTest$data1_pros)
linkingTest$plotMatchingByGroup('birth_year>1850')

linkingTest$full_match
linkingTest$data1_pros
linkingTest$data2_pros
linkingTest$data1
linkingTest$data2

linkingTest$blocks 

# read and save results
linkingTest <- readHismatchClass('linkingTest.RDS')
saveHismatch('linkingTest', 'linkingTest.RDS')
```


## Iterative Matching

There are cases where you want to start with the most restrictive blocking strategy, and than gradually lift blocks for observations that are not matched. In the following, I demonstrate how this can be implemented, where `block_list` is a list of the vectors used for blocking from the strictest to the least strict. This function matches observations in year `t` to `t+1` as defined by the `years` vector.

``` r
linkingTest <- Hismatch$new(firstname = "firstname",
                            surname = "carryforward_surname",
                            dist_thr = 0.7,
                            rel_thr = FALSE,
                            max_block_size = 50000,
                            letters = 1,
                            matching_method = c("jw")
)

# Linking
bl1 <- c("l_first", "l_sur")
bl2 <- c("l_first", "l_sur", "komnr")
bl3 <- c("l_first", "l_sur", "komnr", "residence")
bl4 <- c("l_first", "l_sur", "komnr", "residence", "occupation")
block_list <- list(bl4, bl3, bl2, bl1)

linkingTest$iterative_link_by_year(dta, years, "Norway", "linking_folder", block_list)
```

I will separate out a function to do iterative linking between any two sources in the future. 
