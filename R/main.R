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
                          
                          matching_by_variable = NULL,
                          
                          initialize = function(data1 = NA,  data2 = NA, firstname=NA, surname=NA, 
                                                blocks=NA, dist_thr=0.75, rel_thr=NA) {
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
                            
                            self$matching_by_variable <- NULL
                          }, 
                          
                          addLinkingVariables = function(data_input){
                            data_input[, full_name:=tolower(paste(get(self$firstname, envir = self), get(self$surname, envir = self)))][
                              , l_first := str_match(tolower(get(self$firstname, envir = self)), "(^[:alpha:])")[,1]][
                              !is.na(l_first)][, l_sur := str_match(full_name, " ([:alpha:])[a-zæøå.]*?$")[,2]][
                              !is.na(l_sur)][, masterID := .I]
                          },
                          
                          setNames = function(data_input, suffix){
                            for (j in colnames(data_input)){ 
                              if (j %nin% self$blocks) {setnames(data_input, c(j), paste0(j, suffix))}
                            }
                          },
                          
                          createLinkingData = function(data_input){
                            keep <- append(unlist(strsplit(self$blocks, ", ")), c('full_name', 'masterID'))
                            data_input[,..keep]
                          },
                          
                          createBlocks = function(data){
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
                          
                          fuzzyMatch = function(data1, data2){
                            
                            merge_dataset <- merge(data1, data2, by='block_id', allow.cartesian=TRUE)
                            
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
                          },
                          
                          executeLinking = function(data1, data2, blocks_data) {
                            
                            blocks_tmp <- blocks_data[, block_id:=.I]
                            
                            data1 <- merge(data1, blocks_tmp, by=c(self$blocks), all.y=TRUE)
                            data2 <- merge(data2, blocks_tmp, by=c(self$blocks), all.y=TRUE)
                            
                            data1 <- split(data1, data1$block_id)
                            data2 <- split(data2, data2$block_id)
                            
                            p <- progressr::progressor(steps = nrow(blocks_data))
                            
                            future_map2_dfr(data1, data2, ~{
                              p()
                              self$fuzzyMatch(.x, .y)
                            }, .options = furrr_options(packages=c('data.table', 'stringdist'), globals=FALSE))
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
                            
                            if (is.null(self$merged_data)) {
                              
                              tmp1 <- self$setupData(data_input=copy(self$data1), suffix='_1')  
                              tmp2 <- self$setupData(data_input=copy(self$data2), suffix='_2')
                              
                              self$data1_pros <- data.table::copy(tmp1[[1]])
                              self$data2_pros <- data.table::copy(tmp2[[1]])
                              
                              blocks_tmp <- merge(tmp1[[3]], tmp2[[3]], by=c(self$blocks))
                              
                              merged_data <- with_progress({
                                self$executeLinking(data1=tmp1[[2]], data2=tmp2[[2]], blocks_data=blocks_tmp)
                              })
                              
                              self$merged_data <- copy(merged_data)
                              
                            }else{
                              message("Fuzzy matching already executed. Returns 'old' match with 'new' restrictions.")
                              merged_data <- copy(self$merged_data)
                            }
                            
                            if(self$rel_thr!=FALSE){
                              merged_data <- merged_data[rel1<self$rel_thr | is.na(rel1)][
                                rel2<self$rel_thr | is.na(self$rel2)]}
                            if(self$dist_thr!=FALSE){
                              merged_data <- merged_data[dist>self$dist_thr]}
                            
                            merged_data <- self$mergeBackInData(merged_data, self$data1_pros, self$data2_pros[,c(self$blocks):=NULL])
                            merged_data <- self$vectorReorder(dataset=merged_data)
                            
                            self$full_match <- merged_data
                            
                            invisible(self)
                          }, 
                          
                          getStatistics = function(){
                            merged_data <- self$full_match[,.(masterID_1, masterID_2)]
                            
                            n_matches <- nrow(merged_data)
                            n_data1 <- nrow(self$data1)
                            n_data2 <- nrow(self$data2)
                            
                            message("Summary statistics")
                            output_message <- data.frame (title  = c("no. matches", "share matches (%)"),
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
                            dta <- dta[,dummySummary:=ifelse(is.na(get(m)),0,1)]
                            
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
                          }
                        )
)

#--------------------------------------------------

`%nin%` <- Negate(`%in%`)

`is.not.null` <- Negate(`is.null`)

saveHismatch = function(class_name, filepath){
  code_string <- paste0('saveRDS(', class_name, ', "', filepath, '")')
  eval(parse(text=code_string))
}

readHismatchClass = function(filepath){
  readRDS(here::here(filepath))
}