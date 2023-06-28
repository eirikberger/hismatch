#' R6 Class representing two datasets to be linked.
#'
#' Has information on the two datasets, and the details of how to link them.
#' @export
Hismatch <- R6::R6Class("Hismatch",
                        public = list(
                          data1 = NULL,
                          data2 = NULL,
                          firstname = NULL,
                          surname = NULL,
                          blocks = NULL,
                          dist_thr = NULL,
                          rel_thr = NULL,
                          
                          full_match = NULL,
                          data1_pros = NULL, 
                          data2_pros = NULL, 
                          merged_data = NULL,
                          letters = NULL,
                          matching_method = NULL,
                          
                          matching_by_variable = NULL,
                          max_block_size = NULL,
                          
                          initialize = function(data1 = NA,  data2 = NA, firstname=NA, surname=NA, 
                                                blocks=NA, dist_thr=0.75, rel_thr=NA, max_block_size=50000, 
                                                letters=1, matching_method=c("jw")) {
                            
                            self$data1 <- data.table::copy(data1)
                            self$data2 <- data.table::copy(data2)
                            
                            self$firstname <- firstname
                            self$surname <- surname
                            self$blocks <- blocks
                            self$dist_thr <- dist_thr
                            self$rel_thr <- rel_thr
                            
                            self$full_match <- NULL
                            self$data1_pros <- NULL
                            self$data2_pros <- NULL
                            self$merged_data <- NULL
                            self$letters <- letters
                            self$matching_method <- matching_method
                            
                            self$matching_by_variable <- NULL
                            self$max_block_size <- max_block_size
                          }, 
                          
                          print = function() {
                            print(self$getStatistics())
                          },
                          
                          addLinkingVariables = function(data_input){
                            data_input[, full_name:=tolower(paste(eval(parse(text=self$firstname)), eval(parse(text=self$surname))))][
                              , l_first := stringr::str_match(tolower(eval(parse(text=self$firstname))), paste0("(^[:alpha:]{", self$letters,"})"))[,1]][
                              !is.na(l_first)][, l_sur := stringr::str_match(full_name, paste0(" ([:alpha:]{", self$letters,"})[a-zæøå.]*?$"))[,2]][
                              !is.na(l_sur)][, masterID := .I]
                          },
                          
                          setNames = function(data_input, suffix){
                            for (j in colnames(data_input)){ 
                              if (j %nin% self$blocks) {setnames(data_input, c(j), paste0(j, suffix))}
                            }
                          },
                          
                          createLinkingData = function(data_input){
                            print(self$blocks)
                            keep <- append(unlist(strsplit(self$blocks, ", ")), c('full_name', 'masterID'))
                            data_input[,..keep]
                          },
                          
                          createBlocks = function(data){
                            print(self$blocks)
                            keep <- unlist(strsplit(self$blocks, ", "))
                            comb <- data[,..keep][!is.na(l_first) & !is.na(l_sur)]
                            unique(comb, by = keep)
                          },
                          
                          setupData = function(data_input, suffix){
                            
                            dta_full <- self$addLinkingVariables(data_input=data_input)
                            dta_comp <- self$createLinkingData(dta_full)  
                            self$setNames(dta_full, suffix=suffix)
                            self$setNames(dta_comp, suffix=suffix)
                            blocks <- self$createBlocks(dta_comp)
                            
                            return(list(dta_full, dta_comp, blocks))
                          },
                          
                          fuzzyMatch = function(data1, data2, block_number){
                            
                            data1 <- collapse::fsubset(data1, block_id==block_number)
                            data2 <- collapse::fsubset(data2, block_id==block_number)
                            
                            if (length(data1)*length(data1)>self$max_block_size){
                              stop(paste('Block is too large (l_first: ', data1[['l_first']][1], ', l_sur: ', data1[['l_sur']][1], ')'))
                            }
                            
                            merge_dataset <- merge(data1, data2, by='block_id', allow.cartesian=TRUE)
                            
                            # string distance and execute matching rules
                            merge_dataset[, dist:=stringdist::stringsim(full_name_1, full_name_2, method = self$matching_method)][
                              , rank1:=data.table::frank(-dist, ties.method='max'), by = "masterID_1"][
                              , rank2:=data.table::frank(-dist, ties.method='max'), by = "masterID_2"][
                              order(-dist)][
                              rank1==2, sum1:=collapse::fmean(dist), by="masterID_1"][
                              rank2==2, sum2:=collapse::fmean(dist), by="masterID_2"][
                              , rel1:=collapse::fmean(sum1, na.rm=T)/dist, by="masterID_1"][
                              , rel2:=collapse::fmean(sum2, na.rm=T)/dist, by="masterID_2"][
                              rank1==1 & rank2==1]
                          },
                          
                          executeLinking = function(data1, data2, blocks_data) {
                            
                            blocks_tmp <- blocks_data[, block_id:=.I]
                            
                            data1 <- merge(data1, blocks_tmp, by=c(self$blocks), all.y=TRUE)
                            data2 <- merge(data2, blocks_tmp, by=c(self$blocks), all.y=TRUE)
                            
                            p <- progressr::progressor(steps = collapse::fnrow(blocks_data))
                            
                            future_map_dfr(1:collapse::fnrow(blocks_data), ~{
                              p()
                              self$fuzzyMatch(data1=data1, 
                                              data2=data2, 
                                              block_number=.x)
                            }, .options = furrr_options(globals=FALSE, stdout=FALSE))
                          },
                          
                          mergeBackInData = function(merged_data, master1, master2){
                            skeleton <- merged_data[,.(masterID_1, masterID_2)]
                            skeleton <- merge(skeleton, master1, by='masterID_1')
                            skeleton <- merge(skeleton, master2, by='masterID_2') 
                            merge(merged_data[,.(dist, rel1, rel2, masterID_1, masterID_2)], skeleton, by=c('masterID_1', 'masterID_2'))
                          },
                          
                          vectorReorder = function(dataset){
                            vector_reorder <- colnames(dataset)
                            vector_reorder <- append(self$blocks, vector_reorder[vector_reorder %nin% self$blocks])
                            dataset[,..vector_reorder]
                          },
                          
                          runMatching = function(){
                            
                            status = is.null(self$merged_data)
                            
                            if (status) {
                              
                              tmp1 <- self$setupData(data_input=copy(self$data1), suffix='_1')  
                              tmp2 <- self$setupData(data_input=copy(self$data2), suffix='_2')
                              
                              self$data1_pros <- data.table::copy(tmp1[[1]])
                              self$data2_pros <- data.table::copy(tmp2[[1]])
                              self$data1 <- self$addLinkingVariables(self$data1)
                              self$data2 <- self$addLinkingVariables(self$data2)
                              
                              blocks_tmp <- merge(tmp1[[3]], tmp2[[3]], by=c(self$blocks))
                              
                              progressr::handlers(list(
                                progressr::handler_progress(
                                  format   = ":spin :current/:total (:message) [:bar] :percent in :elapsed ETA: :eta",
                                  width    = 100,
                                  complete = "+"
                                )
                              ))
                              
                              merged_data <- progressr::with_progress({
                                self$executeLinking(data1=tmp1[[2]], data2=tmp2[[2]], blocks_data=blocks_tmp)
                              })
                              
                              self$merged_data <- data.table::copy(merged_data)
                              
                            }else{
                              message("Fuzzy matching already executed. Returns 'old' match with 'new' restrictions.")
                              merged_data <- copy(self$merged_data)
                            }
                            
                            if(self$rel_thr!=FALSE){
                              merged_data <- merged_data[rel1<=self$rel_thr | is.na(rel1)][rel2<=as.integer(self$rel_thr) | is.na(rel2)]
                            }
                            
                            if(self$dist_thr!=FALSE){
                              merged_data <- merged_data[dist>=self$dist_thr]}
                            
                            if (status){
                              merged_data <- self$mergeBackInData(merged_data, self$data1_pros, self$data2_pros[,c(self$blocks):=NULL])
                              merged_data <- self$vectorReorder(dataset=merged_data)
                            }
                            
                            self$full_match <- merged_data
                            
                            invisible(self)
                          }, 
                          
                          getStatistics = function(){
                            merged_data <- self$full_match[,.(masterID_1, masterID_2)]
                            
                            n_matches <- collapse::fnrow(merged_data)
                            n_data1 <- collapse::fnrow(self$data1)
                            n_data2 <- collapse::fnrow(self$data2)
                            
                            message("Summary statistics")
                            output_message <- data.frame(title  = c("no. matches", "share matches (%)"),
                                                         data1 = c(scales::comma(n_data1), scales::percent(n_matches/n_data1, accuracy=0.01)), 
                                                         data2 = c(scales::comma(n_data2), scales::percent(n_matches/n_data2, accuracy=0.01)), 
                                                         matched = c(scales::comma(n_matches), scales::percent(n_matches/n_matches, accuracy=0.01))
                            )
                            
                            return(output_message)
                          },
                          
                          getMatchingRatesByVariable = function(stat_variable, data){
                            
                            l <- names(data);l <- l[grep("^masterID_", l)]
                            m <- ifelse(l=="masterID_1", "masterID_2", "masterID_1")
                            
                            merged_data <- self$full_match[,.(masterID_1, masterID_2)]
                            
                            dta <- merge(data, merged_data, by=l, all=TRUE)
                            dta <- dta[,dummySummary:=ifelse(is.na(dta[[m]]),0,1)]
                            
                            stat <- dta[,.(mean(dummySummary)), by=stat_variable]
                            
                            names(stat) <- c('group', 'matching_rate')
                            
                            stat <- stat[, type:=stat_variable]
                            self$matching_by_variable <- stat[order(group)]
                            
                            invisible(self$matching_by_variable)
                          },
                          
                          getMatchingRatesByVariable_DT = function(variable_list, data){
                            matchingData <- lapply(variable_list, 
                                                   self$getMatchingRatesByVariable, 
                                                   data=data)
                            do.call(rbind.data.frame, matchingData)
                          }, 
                          
                          plotMatchingByGroup = function(type_restriction){
                            ggplot(self$matching_by_variable[type==type_restriction], aes(y=matching_rate, x=group)) + 
                              geom_point() + geom_line() + 
                              scale_y_continuous(labels = scales::percent)
                          },
                          
                          iterative_link = function(data, vector_number, years, output_folder, block_list){
                            
                            print(paste("Linking", years[vector_number], "with", years[vector_number+1]))
                            
                            block <- copy(block_list)
                            
                            if (is.list(block)){
                              self$blocks <- block[[1]]
                            }
                            else{
                              self$blocks <- block
                            }
                            
                            self$data1 <- copy(data[year == years[vector_number]])
                            self$data2 <- copy(data[year == years[vector_number + 1]])
                            
                            # print(self$data1)
                            # print(self$data2)
                            
                            self$runMatching()
                            
                            save_matches <- copy(self$full_match)
                            l1 <- paste0(self$blocks, "_1")
                            l2 <- paste0(self$blocks, "_2")
                            save_matches <- save_matches[, (l1) := .SD, .SDcols = self$blocks]
                            save_matches <- save_matches[, (l2) := .SD, .SDcols = self$blocks]
                            save_matches[, (self$blocks) := NULL]
                            save_matches[, link_type := paste(self$blocks, collapse = "_")]
                            
                            if (is.list(block)){
                              for (block_loop in block[-1]){
                                
                                unmatched1 <- self$data1[!(masterID %in% self$full_match$masterID_1)]
                                unmatched1 <- unmatched1[, l_first := NULL][, l_sur := NULL][, masterID := NULL]
                                unmatched2 <- self$data2[!(masterID %in% self$full_match$masterID_2)]
                                unmatched2 <- unmatched2[, l_first := NULL][, l_sur := NULL][, masterID := NULL]
                                
                                rename_vars(unmatched1, "_1")
                                rename_vars(unmatched2, "_2")
                                
                                self$data1 <- unmatched1
                                self$data2 <- unmatched2
                                self$blocks <- block_loop
                                
                                self$merged_data <- NULL
                                self$runMatching()
                                save_matches_tmp <- copy(self$full_match)
                                
                                l1 <- paste0(block_loop, "_1")
                                l2 <- paste0(block_loop, "_2")
                                save_matches_tmp <- save_matches_tmp[, (l1) := .SD, .SDcols = block_loop]
                                save_matches_tmp <- save_matches_tmp[, (l2) := .SD, .SDcols = block_loop]
                                save_matches_tmp[, (block_loop) := NULL]
                                save_matches_tmp[, link_type := paste(self$blocks, collapse = "_")]
                                
                                save_matches <- rbind(save_matches, save_matches_tmp, fill=TRUE)
                                print(paste("Added", nrow(self$full_match), "matches"))
                              }
                            }
                            
                            ###
                            
                            # save merge statistics
                            merge_statistics <- setDT(getStatistics(save_matches, data[year == years[vector_number]], data[year == years[vector_number+1]]))
                            merge_statistics[, from := years[vector_number]]
                            merge_statistics[, to := years[vector_number+1]]
                            merge_statistics[, type := name_string]
                            
                            # unmatached
                            unmatched1 <- self$data1[!(masterID %in% self$full_match$masterID_1)]
                            unmatched1 <- unmatched1[, l_first := NULL][, l_sur := NULL][, masterID := NULL]
                            unmatched2 <- self$data2[!(masterID %in% self$full_match$masterID_2)]
                            unmatched2 <- unmatched2[, masterID := NULL]
                            
                            # Save results
                            file1 <- paste0(output_folder, "/link_", as.character(name_string), "_", years[vector_number], "_to_",  years[vector_number+1],".csv")
                            file2 <- paste0(output_folder, "/stat_", as.character(name_string), "_", years[vector_number], "_to_",  years[vector_number+1],".csv")
                            file3 <- paste0(output_folder, "/unmatched_", as.character(name_string), "_", years[vector_number], "_to_",  years[vector_number+1],".csv")
                            
                            # iteratively bind the results rowwise
                            fwrite(merge_statistics, file2, sep = ";")
                            fwrite(save_matches, file1, sep = ";")
                            fwrite(unmatched1, file3, sep = ";")
                          },
                          
                          iterative_link_by_year = function(data, years, name_string, output_folder, block_list) {
                            
                            for(i in 1:(length(years) - 1)){
                              self$iterative_link(data, i, years, output_folder, block_list)
                            }
                          }
                        )
)

#--------------------------------------------------

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

#' @export
rename_vars <- function(dt, suffix) {
  old_names <- names(dt)
  rename_vars <- grep(paste0(suffix, "$"), old_names, value = TRUE)
  new_names <- gsub(paste0(suffix, "$"), "", rename_vars)
  setnames(dt, old = rename_vars, new = new_names)
  return(dt)
}

#' @export
getStatistics = function(full_match, data1, data2){
  merged_data <- full_match[,.(masterID_1, masterID_2)]
  
  n_matches <- collapse::fnrow(merged_data)
  n_data1 <- collapse::fnrow(data1)
  n_data2 <- collapse::fnrow(data2)
  
  message("Summary statistics")
  output_message <- data.frame(title  = c("no. matches", "share matches (%)"),
                               data1 = c(scales::comma(n_data1), scales::percent(n_matches/n_data1, accuracy=0.01)), 
                               data2 = c(scales::comma(n_data2), scales::percent(n_matches/n_data2, accuracy=0.01)), 
                               matched = c(scales::comma(n_matches), scales::percent(n_matches/n_matches, accuracy=0.01))
  )
  
  return(output_message)
}