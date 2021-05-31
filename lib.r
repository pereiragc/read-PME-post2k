pme_ftp_baseurl <- "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Mensal_de_Emprego/Microdados"

pme_download <- function(baseurl = pme_ftp_baseurl) {
  obj <- list(baseurl = baseurl)

  class(obj) <- append(class(obj), "pmedl")

  return(obj)
}

getYearFiles <- function(pmemethod, yyyy) {
  UseMethod("getYearFiles", pmemethod)
}

getYearFiles.pmedl <- function(pmedl, yyyy) {
  year_ftp <- paste(pmedl$baseurl, yyyy, sep = "/")

  ## Add forward slash to end if not there
  year_ftp <- gsub("(.*?)/*$", "\\1/", year_ftp)

  ll <- readFtpDir(year_ftp)

  return(file_list(year_ftp, ll, remote = TRUE))
}

file_list <- function(base_dir, filenames, remote) {
  base_dir <- gsub("(.*?)/*$", "\\1", base_dir)
  flist <- list(base_dir = base_dir,
       filenames = filenames,
       remote = remote)

  class(flist) <- append(class(flist), "filelist")
  return(flist)
}

collect <- function(filelist) paste(filelist$base_dir, filelist$filenames, sep = "/")


readFtpDir <- function(addr) {
  ## https://gist.github.com/adamhsparks/18f7702906f33dd66788e0078979ff9a

  list_files <- curl::new_handle()
  curl::handle_setopt(list_files, ftp_use_epsv = TRUE, dirlistonly = TRUE)

  con <- curl::curl(url = addr, "r", handle = list_files)
  ff <- readLines(con)
  close(con)

  return(ff)
}



handleFiles <- function(pmemethod, flist, user_parameters, yyyy) {
  UseMethod("handleFiles", pmemethod)
}

#' Downloads PME year & reads it into memory
handleFiles.pmedl <- function(pmedl, flist, user_parameters, yyyy) {
  tmpd <- tempdir()

  lDT <- (lapply(collect(flist), function(f) {
    tmpf <- tempfile()
    curl::curl_download(f, tmpf)
    message(glue::glue("[Year {yyyy}]   Downloaded file {f}"))
    u <- unzip(tmpf, exdir = tmpd)


    DT <- .readfun2k(u, yyyy, dict_post2k)

    file.remove(c(tmpf, u))
    return(DT)
  }))

  return(data.table::rbindlist(lDT))
}


.readfun2k <- function(fname, yyyy, dict) {
  df <- .readfun(fname, dict)
  df[, .year := yyyy]
  message(glue::glue("[Year {yyyy}]     Read {fname}"))
  return(df)
}


.readfun <- function(fname, dict) {
  DT <- fread(fname, header = FALSE, sep = "\n")
  DT <- DT[, lapply(seq_len(dict[, .N]), function(ii) {
    stringi::stri_sub(V1, dict[, Start[ii]], dict[, End[ii]])
    ## https://stackoverflow.com/questions/24715894/faster-way-to-read-fixed-width-files
  })]
  data.table::setnames(DT, new = dict[, Name])
}
