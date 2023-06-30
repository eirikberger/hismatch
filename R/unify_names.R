library(data.table)
library(stringr)

#' Unify the format of two names based on their middle names
#'
#' @param name1 A character vector of the first names.
#' @param name2 A character vector of the second names.
#'
#' @return A list with two elements: `name1` and `name2`, which are the modified versions of the input names.
#'
#' @export
unify_names <- function(name1, name2) {
  
  # Initialize vectors for storing the results
  result_name1 <- vector("character", length(name1))
  result_name2 <- vector("character", length(name2))
  
  # Loop over each pair of names
  for(i in seq_along(name1)) {
    
    # Split the names into words
    words1 <- str_split(name1[i], " ")[[1]]
    words2 <- str_split(name2[i], " ")[[1]]
    
    # If the first or last word in either name is too short, return NA
    if (nchar(words1[1]) <= 1 | nchar(words1[length(words1)]) <= 1 | 
        nchar(words2[1]) <= 1 | nchar(words2[length(words2)]) <= 1) {
      result_name1[i] <- NA_character_
      result_name2[i] <- NA_character_
      next
    }
    
    # Get the middle words of the names
    middle1 <- words1[-c(1, length(words1))]
    middle2 <- words2[-c(1, length(words2))]
    
    # If one name has no middle word, remove the middle word from the other
    if (length(middle1) == 0 | length(middle2) == 0) {
      result_name1[i] <- paste(words1[1], words1[length(words1)], sep = " ")
      result_name2[i] <- paste(words2[1], words2[length(words2)], sep = " ")
      next
    }
    
    # If both names have full middle names, leave them as they are
    if (all(nchar(middle1) > 1) & all(nchar(middle2) > 1)) {
      result_name1[i] <- name1[i]
      result_name2[i] <- name2[i]
      next
    }
    
    # If one name has abbreviated middle names, abbreviate the middle names in the other name
    if (any(nchar(middle1) == 1) | any(nchar(middle2) == 1)) {
      result_name1[i] <- paste(c(words1[1], str_sub(middle1, 1, 1), words1[length(words1)]), collapse = " ")
      result_name2[i] <- paste(c(words2[1], str_sub(middle2, 1, 1), words2[length(words2)]), collapse = " ")
      next
    }
  }
  
  # Return the results as a list
  return(list(result_name1, result_name2))
}

