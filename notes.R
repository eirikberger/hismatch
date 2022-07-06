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