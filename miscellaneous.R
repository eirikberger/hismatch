#Code chunks
hismatch_settings <- function(...){
    hismatch_env <<- rlang::env(...)
}

hismatch_settings(data1="t1",
    data2="t2",
    firstname='firstname',
    surname='surname',
    blocks=c('l_first', 'l_sur', 'byear', 'bmonth', 'male'),
    dist_thr=0.75, 
    rel_thr=FALSE, 
    workers_input=2
)

print.hismatch <- function(){
    hismatch_env$blocks
}

print.hismatch()

change.hismatch <- function(){
    hismatch_env$blocks <- "TEST"
}

change.hismatch()

hismatch_env


# Notes
library(memoise)

# cache function
get_cache <- function(x){
  Sys.sleep(x)
  return(x)
}

get_cache(5)

cd <- cachem::cache_disk(rappdirs::user_cache_dir("hismatch"))

get_cache_m <- memoise::memoise(get_cache, cache=cd)

is.memoised(get_cache_m)

get_cache_m(5)