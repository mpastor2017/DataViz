# Author: marip

library(data.table)
library(anytime)
library(tm)

listings = fread("listings.csv")

# Text Mining Listings Data

# Gathering columns needed for text analysis
listing_text = listings[,c(1,5:8,11:12,
                                22,32,56)]

# Creating a data table with all the metrics by ID
listing_metrics = listings[,1]

#Prior to cleaning script, we can gather some features from the text

# Length metrics

#Number of characters
listing_metrics$length_of_listing_name = apply(listing_text[,2], 1, nchar)

#  3 Weird listing names, and the max number of characters should only be 35
listing_text[which(listing_metrics$length_of_listing_name > 50),2]
listing_metrics$length_of_listing_name = ifelse(listing_metrics$length_of_listing_name > 35, 35,
                                               listing_metrics$length_of_listing_name)

listing_metrics$length_of_summary = apply(listing_text[,3], 1, nchar)

listing_metrics$length_of_space = apply(listing_text[,4], 1, nchar)
# Noticed that > half of space descriptions had length of 1000,
# changed any description larger than 1000 to 1000
listing_metrics$length_of_space = ifelse(listing_metrics$length_of_space > 1000, 1000,
                                               listing_metrics$length_of_space)

listing_metrics$length_of_description = apply(listing_text[,5], 1, nchar)
# Noticed that > half of descriptions had length of 1000,
# changed any description larger than 1000 to 1000
listing_metrics$length_of_description = ifelse(listing_metrics$length_of_description > 1000, 1000,
                                               listing_metrics$length_of_description)

listing_metrics$length_of_note = apply(listing_text[,6], 1, nchar)
listing_metrics$length_of_transit = apply(listing_text[,7], 1, nchar)

listing_metrics$length_of_host_about = apply(listing_text[,8], 1, nchar)

# Number of words

listing_metrics$nb_words_name = sapply(gregexpr("[[:alpha:]]+", listing_text$name),
                                       function(x) sum(x > 0))
listing_metrics$nb_words_summary = sapply(gregexpr("[[:alpha:]]+", listing_text$summary),
                                          function(x) sum(x > 0))

listing_metrics$nb_words_space = sapply(gregexpr("[[:alpha:]]+", listing_text$space),
                                       function(x) sum(x > 0))
listing_metrics$nb_words_description = sapply(gregexpr("[[:alpha:]]+", listing_text$description),
                                          function(x) sum(x > 0))

listing_metrics$nb_words_notes = sapply(gregexpr("[[:alpha:]]+", listing_text$notes),
                                          function(x) sum(x > 0))

listing_metrics$nb_words_transit = sapply(gregexpr("[[:alpha:]]+", listing_text$transit),
                                        function(x) sum(x > 0))
listing_metrics$nb_words_host_about = sapply(gregexpr("[[:alpha:]]+", listing_text$host_about),
                                              function(x) sum(x > 0))
# Writing to CSVs for Python
fwrite(listing_metrics, "metrics.csv")
fwrite(listing_text, "text.csv")


cleaned_text = apply(listing_text[,-c(1,9,10)],2,function(x) {
        x <- tolower(x)
        x <- gsub("<img src.*?>", "", x)
        x <- gsub("http\\S+", "", x)
        x <- gsub("\\[math\\]", "", x) # text between [] refers to tags e.g. [math]
        x <- gsub("<.*?>", " ", x)
        x <- gsub("\n", " ", x)  # replace newline with a space
        x <- gsub("\\s+", " ", x)  # multiple spaces into one
        # using tm_map to remove stopwords
        docs <- Corpus(VectorSource(x))
        docs <- tm_map(docs, removeWords, stopwords('en'))
        docs <- tm_map(docs, removePunctuation)    # dont remove punct so early in the analysis
        docs <- tm_map(docs, stripWhitespace)
        xxx <- sapply(docs, function(i) i)
        data_content <- data.frame(text = xxx, stringsAsFactors = FALSE)
        data_content$text <- gsub("\\d+", "", data_content$text)})

cleaned_text = data.table(cleaned_text)
cleaned_text = cbind(listing_metrics$id, cleaned_text)
fwrite(cleaned_text, "cleaned_text.csv")


## Metrics after python
metrics_final = fread("new_metrics.csv")


# Removing first column
metrics_final = metrics_final[, V1 := NULL]

# Removing Erroneous columns
metrics_final = metrics_final[, amenities_nb_sent := NULL]
metrics_final = metrics_final[, host_verifications_nb_sent := NULL]


fwrite(metrics_final, "metrics_final.csv")

metrics_final = fread("metrics_final.csv")


# Removing name_nb_sentence
metrics_final = metrics_final[, name_nb_sent:= NULL]

# Readability score
metrics_final$summary_ARI = 4.71 * (metrics_final$length_of_summary/
                                         metrics_final$nb_words_summary) +
                                        0.5 * (metrics_final$nb_words_summary/metrics_final$summary_nb_sent) -
                                        21.43

metrics_final$space_ARI = 4.71 * (metrics_final$length_of_space/
                                        metrics_final$nb_words_space) +
                                        0.5 * (metrics_final$nb_words_space/metrics_final$space_nb_sent) -
                                         21.43

metrics_final$description_ARI = 4.71 * (metrics_final$length_of_description/
                                          metrics_final$nb_words_description) +
                                                0.5 * (metrics_final$nb_words_description/metrics_final$description_nb_sent) -
                                                21.43

fwrite(metrics_final, "metrics_final.csv")
# Consider normalizing









