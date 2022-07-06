#' Create variables to link on
#' @export
addLinkingVariables <- function(data_input, firstname_input, lastname_input){
  data_input[, full_name:=tolower(paste(get(..firstname_input), get(..lastname_input)))][
    , l_first := str_match(tolower(get(..firstname_input)), "(^[:alpha:])")[,1]][
    !is.na(l_first)][
    , l_sur := str_match(full_name, " ([:alpha:])[a-zæøå.]*?$")[,2]][
    !is.na(l_sur)][
    , masterID := .I]
}

#' Names names ('_1' and '_2')
#' @export
setNames <- function(data_input, suffix, block=c('')){
  for (j in colnames(data_input)){ 
    if (j %nin% block) {setnames(data_input, c(j), paste0(j, suffix))}
  }
}

#' Create parsimonious dataset for linking
#' @export
createLinkingData <- function(data_input, block=c('')){
  keep <- append(unlist(strsplit(block, ", ")), c('full_name', 'masterID'))
  data_input[,..keep]
}

#' Create blocks
#' @export
createBlocks <- function(data, block=c('')){
  keep <- unlist(strsplit(block, ", "))
  comb <- data[,..keep][!is.na(l_first) & !is.na(l_sur)]
  unique(comb, by = keep)
}

#' Setup for linking using other functions
#' @export
setupData <- function(data, firstname, surname, block_vector, suffix){
  
  dta_full <- hismatch:::addLinkingVariables(data_input=data, 
                                   firstname_input=paste0(firstname), 
                                   lastname_input=paste0(firstname))
  
  dta_comp <- hismatch:::createLinkingData(dta_full,
                                 block=block_vector)
  
  hismatch:::setNames(dta_full, suffix=suffix, block=block_vector)
  hismatch:::setNames(dta_comp, suffix=suffix, block=block_vector)
  
  blocks <- hismatch:::createBlocks(dta_comp, 
                         block=block_vector)
  
  return(list(dta_full, dta_comp, blocks))
}

#' Fuzzy mathing
#' @export
fuzzyMatch<- function(data1, data2, block_number, blocks_tmp, dist_thr, rel_thr, blocks){

  for (blocks_variable in blocks) {
    data1 <- data1[blocks_variable==paste0(blocks_tmp[[c(block_number), blocks_variable]])]
    data2 <- data2[blocks_variable==paste(blocks_tmp[[c(block_number), blocks_variable]])]
  }
  
  merge_dataset <- merge(data1, data2, by=blocks, allow.cartesian=TRUE)
  
  # string distance and execute matching rules
  merge_dataset <- merge_dataset[,dist:=stringdist::stringsim(full_name_1, full_name_2, method = c("jw"))][
    , rank1 := frank(-dist, ties.method='max'), by = "masterID_1"][
    , rank2 := frank(-dist, ties.method='max'), by = "masterID_2"][
    order(-dist)]
  
  merge_dataset <- merge_dataset[
    rank1==2, sum1:=mean(dist), by="masterID_1"][
    rank2==2, sum2:=mean(dist), by="masterID_2"][
    , rel1:=mean(sum1, na.rm=T)/dist, by="masterID_1"][
    , rel2:=mean(sum2, na.rm=T)/dist, by="masterID_2"]
  
  merge_dataset[
    rank1==1 & rank2==1]
}

#' Loop over fuzzy matching using the future package
#' @export
executeLinking <- memoise::memoise(function(data1, data2, blocks_data, 
        dist_thr, rel_thr, blocks) {
  p <- progressor(steps = nrow(blocks_data))
  
  future_map_dfr(1:nrow(blocks_data), ~{
    p()
    hismatch:::fuzzyMatch(data1=data1, 
               data2=data2, 
               block_number=.x, 
               blocks_tmp=blocks_data,
               dist_thr=dist_thr, 
               rel_thr=rel_thr, 
               blocks=blocks)
  })
}, cache=cachem::cache_disk(rappdirs::user_cache_dir("hismatch")))

#' Merge back in full data
#' @export
mergeBackInData <- function(merged_data, master1, master2){
  skeleton <- merged_data[,.(masterID_1, masterID_2)]
  skeleton <- merge(skeleton, master1, by='masterID_1')
  skeleton <- merge(skeleton, master2, by='masterID_2')
  merge(merged_data[,.(dist, rel1, rel2, masterID_1, masterID_2)], skeleton, by=c('masterID_1', 'masterID_2'))
}

#' Reorder variables
#' @export
vectorReorder <- function(dataset, blocks){
  vector_reorder <- colnames(dataset)
  vector_reorder <- append(blocks, vector_reorder[vector_reorder %nin% blocks])
  dataset[,..vector_reorder]
}

#' Run all
#' @export
run <- function(data1, data2, firstname=NULL, surname=NULL, 
                firstname1=NULL, surname1=NULL, 
                firstname2=NULL, surname2=NULL,
                dist_thr, rel_thr, blocks, workers_input=1){
  
  future::plan(multisession, workers = workers_input)

  if (is.not.null(firstname) & is.not.null(surname)) {
    firstname1 <- firstname;firstname2 <- firstname
    surname1 <- surname;surname2 <-surname
  }

  tmp1 <- hismatch:::setupData(data=data1,
                     firstname=paste0(firstname1), 
                     surname=paste0(surname1), 
                     block_vector = blocks, 
                     suffix='_1')
  
  
  tmp2 <- hismatch:::setupData(data=data2,
                     firstname=paste0(firstname2), 
                     surname=paste0(surname2), 
                     block_vector = blocks, 
                     suffix='_2')
  
  
  blocks_tmp <- merge(tmp1[[3]], tmp2[[3]], by=c(blocks))
  
  merged_data <- with_progress({
    hismatch:::executeLinking(data1=tmp1[[2]], 
                   data2=tmp2[[2]],
                   blocks_data=blocks_tmp, 
                   dist_thr=dist_thr, 
                   rel_thr=rel_thr, 
                   blocks=blocks)
  })

  if(rel_thr!=FALSE){merged_data <- merged_data[rel1<rel_thr | is.na(rel1)][rel2<rel_thr | is.na(rel2)]}
  if(dist_thr!=FALSE){merged_data <- merged_data[dist>dist_thr]}
  
  merged_data <- hismatch:::mergeBackInData(merged_data, tmp1[[1]], tmp2[[1]][,c(blocks):=NULL])
  merged_data <- hismatch:::vectorReorder(dataset=merged_data, blocks=blocks)

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