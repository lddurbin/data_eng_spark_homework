library(sparklyr)

zip_file <- fs::dir_ls(here::here("data"), .glob = "*.csv.gz")
fhv_2019 <- readr::read_csv(zip_file)

sc <- spark_connect(master = "local", spark_home = "/usr/local/Cellar/apache-spark/3.5.0/libexec")

sparklyr::spark_version(sc) # Question 1

tbl_fhv <- copy_to(sc, fhv_2019, "spark_fhv2019")

fhv_partitioned <- sdf_repartition(tbl_fhv, 6)

parquet_dir <- here::here("data/fhv/2019/10/")

sparklyr::spark_write_parquet(fhv_partitioned, parquet_dir, mode = "overwrite")

file_info <- file.info( fs::dir_ls(parquet_dir, glob = "*.snappy.parquet"))

mean(file_info$size) #Question 2

tbl_fhv_partitioned <- spark_read_parquet(sc, name = "spark_fhv2019_partitioned",
                                          path = parquet_dir)

oct_15 <- tbl_fhv_partitioned |>
  filter(
    pickup_datetime >= as.Date("2019-10-15"),
    pickup_datetime < as.Date("2019-10-16")
    ) |>
  collect()

spark_disconnect(sc)
