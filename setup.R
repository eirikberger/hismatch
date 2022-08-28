#git clone git@github.com:eirikberger/hismatch.git

library(devtools)

options(
  usethis.full_name = "Eirik Berger",
  usethis.description = list(
    `Authors@R` = 'person("Eirik", "Berger", email = "eirik.berger@gmail.com", role = c("aut", "cre"))',
    License = "MIT + file LICENSE",
    Version = "0.1"
))

create_package("hismatch")

#load_all()
#check()
#document()
#install()
#use_package("")

packages <- c('future', 'stringr', 'plyr', 'data.table', 'stringdist', 'furrr', 'progress',
               'purrr', 'progressr', 'ggsci', 'scales', 'ggplot2')

#for (i in packages){
#  use_package(i)
#}

devtools::check()
#devtools::document()
devtools::install()