
library(glue)
library(data.table)
library(readxl)
library(curl)


source("lib.r", chdir = TRUE)

user_parameters <- list(
  read_years = seq(2002, 2015),
  out_dir = "/home/gustavo/Dropbox/v2/data/PME/FullData",
  dict_path = "input/dict-2k-2015.csv",
  method = pme_download()
)

dict_post2k <- data.table::fread("input/dict-2k-2015.csv", colClasses = list(character = "Width"))
dict_post2k[grepl("\\.\\d+", Width), Decimal := gsub(".*\\.(\\d+)", "\\1", Width)]
dict_post2k[, Width := as.integer(floor(as.numeric(Width)))]
dict_post2k[, End :=  Start + Width - 1]

post01 <- intersect(seq(2002, 2015, by = 1), user_parameters$read_years)

for (yyyy in as.character(post01)) {

  ## returns a `filelist` object
  ff <- getYearFiles(user_parameters$method, yyyy)

  ## Parse & save data for that year in memory
  DT <- handleFiles(user_parameters$method, ff, user_parameters, yyyy)

  ## Save to disk

  fwrite(DT, file.path(user_parameters$out_dir,glue::glue( "full-{yyyy}.csv")))
  message(glue::glue("[Year {yyyy}] Wrote parsed datasets"))
}
