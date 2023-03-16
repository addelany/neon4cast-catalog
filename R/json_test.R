library(httr)

collection <- jsonlite::read_json("output.json")

collection$license

#providers
for (i in seq.int(1, length(collection$providers))){
  print(i)
  provider_info <- c(collection$providers[[i]]$name, collection$providers[[i]]$url, c(unlist(collection$providers[[i]]$roles)))
  print(provider_info)
}

## assets

asset_title <- c()
asset_href <- c()
asset_type <- c()
asset_roles <- c()
asset_description <- c()

for (i in seq.int(1,length(list(collection$assets$parquet_items)))){ ## this needs to be converted to list before this step (do in build_collection)
  asset_title[i] <- list(collection$assets$parquet_items)[[i]]$title
  asset_href[i] <- list(collection$assets$parquet_items)[[i]]$href
  asset_type[i] <- list(collection$assets$parquet_items)[[i]]$type
  asset_roles[i] <- list(collection$assets$parquet_items)[[i]]$roles[[1]]
  asset_description[i] <- list(collection$assets$parquet_items)[[i]]$description

}

asset_df <- data.frame(asset_title,
                       asset_href,
                       asset_type,
                       asset_roles,
                       asset_description)

asset_table <- reactable(asset_df,
                      defaultColDef = colDef(
                        align = "left"),
                      columns = list(asset_title = colDef(name='Title'),
                                     asset_href = colDef(name='Href'),
                                     asset_type = colDef(name = 'Type'),
                                     asset_roles = colDef(name = 'Roles'),
                                     asset_description = colDef(name = 'description')),
                      defaultPageSize = 10,
                      filterable = FALSE,
                      highlight = TRUE)

asset_table



## COLUMNS
collection$table_columns

tc_name <- c()
tc_type <- c()
tc_description <- c()

for (i in seq.int(1, length(collection$table_columns))){
  tc_name[i] <- collection$table_columns[[i]]$name
  tc_type[i] <- collection$table_columns[[i]]$type
  tc_description[i] <- collection$table_columns[[i]]$description
}

tc_df <- data.frame(tc_name,
                    tc_description,
                    tc_type)

columns_table <- reactable(tc_df,
                         defaultColDef = colDef(
                           align = "left"),
                         columns = list(tc_name = colDef(name='Name'),
                                        tc_description = colDef(name='Description'),
                                        tc_type = colDef(name = 'Type')),
                         defaultPageSize = 10,
                         filterable = FALSE,
                         highlight = TRUE)

columns_table



## Publications
pub_doi <- c()
pub_cite <- c()

for (i in seq.int(1, length(collection$publications))){
  pub_doi[i] <- collection$publications[[i]]$doi
  pub_cite[i] <- collection$publications[[i]]$citation
}

pub_df <- data.frame(pub_doi,
                    pub_cite)

pub_table <- reactable(pub_df,
                           defaultColDef = colDef(
                             align = "left"),
                           columns = list(pub_doi = colDef(name='DOI'),
                                          pub_cite = colDef(name='Citation')),
                           defaultPageSize = 10,
                           filterable = FALSE,
                           highlight = TRUE)

pub_table
