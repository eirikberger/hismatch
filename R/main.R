#' Create enviroment with input information
#' @export
hismatch_settings <- function(...){
    .hismatch_env <<- rlang::env(..., parent = emptyenv())
}

#' Create variables to link on
#' @export
addLinkingVariables <- function(data_input){
  data_input[, full_name:=tolower(paste(get(.hismatch_env$firstname), get(.hismatch_env$surname)))][
    , l_first := str_match(tolower(get(.hismatch_env$firstname)), "(^[:alpha:])")[,1]][
    !is.na(l_first)][, l_sur := str_match(full_name, " ([:alpha:])[a-zæøå.]*?$")[,2]][
    !is.na(l_sur)][, masterID := .I]
}

#' Names names ('_1' and '_2')
#' @export
setNames <- function(data_input, suffix){
  for (j in colnames(data_input)){ 
    if (j %nin% .hismatch_env$blocks) {setnames(data_input, c(j), paste0(j, suffix))}
  }
}

#' Create parsimonious dataset for linking
#' @export
createLinkingData <- function(data_input){
  keep <- append(unlist(strsplit(.hismatch_env$blocks, ", ")), c('full_name', 'masterID'))
  data_input[,..keep]
}

#' Create blocks
#' @export
createBlocks <- function(data){
  keep <- unlist(strsplit(.hismatch_env$blocks, ", "))
  comb <- data[,..keep][!is.na(l_first) & !is.na(l_sur)]
  unique(comb, by = keep)
}

#' Setup for linking using other functions
#' @export
setupData <- function(data_input, suffix){
  
  dta_full <- hismatch:::addLinkingVariables(data_input=data_input)
  dta_comp <- hismatch:::createLinkingData(dta_full)  
  hismatch:::setNames(dta_full, suffix=suffix)
  hismatch:::setNames(dta_comp, suffix=suffix)
  blocks <- hismatch:::createBlocks(dta_comp)
  
  return(list(dta_full, dta_comp, blocks))
}

#' Fuzzy mathing
#' @export
fuzzyMatch<- function(data1, data2, block_number, blocks_tmp, blocks){
    
  stopifnot(is.data.table(data1) & is.data.table(data2))

  for (blocks_variable in blocks) {
    ex <- blocks_tmp[[c(block_number), blocks_variable]]
    data1 <- subset(data1, get(blocks_variable)==paste0(ex))
    data2 <- subset(data2, get(blocks_variable)==paste0(ex))
  }
    
  merge_dataset <- merge(data1, data2, by=blocks, allow.cartesian=TRUE)
  
  stopifnot(is.data.table(merge_dataset))

  # string distance and execute matching rules
  merge_dataset <- merge_dataset[, dist:=stringsim(full_name_1, full_name_2, method = c("jw"))]
  merge_dataset <- merge_dataset[
    , rank1:=frank(-dist, ties.method='max'), by = "masterID_1"][
    , rank2:=frank(-dist, ties.method='max'), by = "masterID_2"][
    order(-dist)]
  
  merge_dataset <- merge_dataset[
    rank1==2, sum1:=mean(dist), by="masterID_1"][
    rank2==2, sum2:=mean(dist), by="masterID_2"][
    , rel1:=mean(sum1, na.rm=T)/dist, by="masterID_1"][
    , rel2:=mean(sum2, na.rm=T)/dist, by="masterID_2"]
  
  merge_dataset[rank1==1 & rank2==1]
}

#' Loop over fuzzy matching using the future package
#' @export
executeLinking <- memoise::memoise(function(data1, data2, blocks_data) {

  p <- progressr::progressor(steps = nrow(blocks_data))
  
  future_map_dfr(1:nrow(blocks_data), ~{
    p()
    hismatch:::fuzzyMatch(data1=data1, 
               data2=data2, 
               block_number=.x, 
               blocks_tmp=blocks_data, 
               blocks=.hismatch_env$blocks)
  }, .options = furrr_options(packages=c("data.table", "stringdist")))
}, cache=cachem::cache_disk(rappdirs::user_cache_dir("hismatch")))

#' Merge back in full datas
#' @export
mergeBackInData <- function(merged_data, master1, master2){
  skeleton <- merged_data[,.(masterID_1, masterID_2)]
  skeleton <- merge(skeleton, master1, by='masterID_1')
  skeleton <- merge(skeleton, master2, by='masterID_2')
  merge(merged_data[,.(dist, rel1, rel2, masterID_1, masterID_2)], skeleton, by=c('masterID_1', 'masterID_2'))
}

#' Reorder variables
#' @export
vectorReorder <- function(dataset){
  vector_reorder <- colnames(dataset)
  vector_reorder <- append(.hismatch_env$blocks, vector_reorder[vector_reorder %nin% .hismatch_env$blocks])
  dataset[,..vector_reorder]
}

#' Run all
#' @export
runMatching <- function(){
  
  plan(multisession, workers = .hismatch_env$workers)

  tmp1 <- hismatch:::setupData(data_input=.hismatch_env$data1, suffix='_1')  
  tmp2 <- hismatch:::setupData(data_input=.hismatch_env$data2, suffix='_2')
  
  blocks_tmp <- merge(tmp1[[3]], tmp2[[3]], by=c(.hismatch_env$blocks))
  
  merged_data <- with_progress({
    hismatch:::executeLinking(data1=tmp1[[2]], data2=tmp2[[2]], blocks_data=blocks_tmp)
  })

  if(.hismatch_env$rel_thr!=FALSE){
      merged_data <- merged_data[rel1<.hismatch_env$rel_thr | is.na(rel1)][
      rel2<.hismatch_env$rel_thr | is.na(.hismatch_env$rel2)]}
  if(.hismatch_env$dist_thr!=FALSE){
      merged_data <- merged_data[dist>.hismatch_env$dist_thr]}
  
  merged_data <- hismatch:::mergeBackInData(merged_data, tmp1[[1]], tmp2[[1]][,c(.hismatch_env$blocks):=NULL])
  merged_data <- hismatch:::vectorReorder(dataset=merged_data)

  return(list(merged_data, tmp1[[1]], tmp2[[1]]))
}




#' Produce matching statistics
#' @export
getStatistics <- function(merged_data, data1, data2){
  merged_data <- merged_data[,.(masterID_1, masterID_2)]
  
  n_matches <- nrow(merged_data)
  n_data1 <- nrow(data1)
  n_data2 <- nrow(data2)
  
  message("Summary statistics")
  output_message <- data.frame (title  = c("no. matches", "share matches (%)"),
                                data1 = c(scales::comma(n_data1), scales::percent(n_matches/n_data1, accuracy=0.01)), 
                                data2 = c(scales::comma(n_data2), scales::percent(n_matches/n_data2, accuracy=0.01)), 
                                matched = c(scales::comma(n_matches), scales::percent(n_matches/n_matches, accuracy=0.01))
  )
  
  return(output_message)
}

#' Produce matching statistics by variable
#' @export
getMatchingRatesByVariable <- function(merged_data, data, stat_variable){

  l <- names(data);l <- l[grep("^masterID_", l)]
  m <- ifelse(l=="masterID_1", "masterID_2", "masterID_1")

  merged_data <- merged_data[,.(masterID_1, masterID_2)]

  dta <- merge(data, merged_data, by=l, all=TRUE)
  dta <- dta[,dummySummary:=ifelse(is.na(get(m)),0,1)]

  stat <- dta[,.(mean(dummySummary)), by=stat_variable]
  
  names(stat) <- c('group', 'matching_rate')
  stat[, type:=stat_variable][, matching_rate:=matching_rate]
}

#' Plot matching statistics by variable
#' @export
plotMatchingByGroup <- function(matching_data, type_restriction){
  ggplot(matching_data[type==type_restriction], aes(y=matching_rate, x=group)) + 
  geom_point() + geom_line() + 
  scale_y_continuous(labels = scales::percent)
}

#' Convert list of matching statistics from 'plotMatchingByGroup' to data.frame
#' @export
getMatchingRatesByVariable_DT <- function(variable_list, merged_data, data){
  matchingData <- lapply(variable_list, hismatch:::getMatchingRatesByVariable, 
    merged_data=merged_data, 
    data=data)
  do.call(rbind.data.frame, matchingData)
}