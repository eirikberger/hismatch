


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
