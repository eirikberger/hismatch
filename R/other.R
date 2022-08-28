#' @export
`%nin%` <- Negate(`%in%`)

#' @export
`is.not.null` <- Negate(`is.null`)

#' @export
saveHismatch = function(class_name, filepath){
  code_string <- paste0('saveRDS(', class_name, ', "', filepath, '")')
  eval(parse(text=code_string))
}

#' @export
readHismatchClass = function(filepath){
  readRDS(here::here(filepath))
}