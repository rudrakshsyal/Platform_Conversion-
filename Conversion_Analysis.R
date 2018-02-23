# Aim to analyze conversion% from different dimensions such as -
  # 1. speciality
  # 2. locality
  # 3. platform
  # 4. mode
  # 5. doctor-level attributes (consultation_fee, reviews, recommendations, etc.)

##### LOADING LIBRARIES -----

suppressMessages(source("./load_libraries.R"))
library(RMixpanel)

##### SETTING UP CONNECTION TO THE DATABASE ----

source("./DbCreds.R")

##### CITY-LOCALITY-DOCTOR_ID MAPPING ----
mapping <- dbGetQuery(conn2,
"select 
pd.doctor_id as doctor_id, 
c.name as city, 
l.name as locality
FROM fabric_pii.practice_doctors_published pd
JOIN fabric_pii.practices_published p ON p.id = pd.practice_id
LEFT JOIN fabric_pii.master_localities l ON p.locality_id = l.id
LEFT JOIN fabric_pii.master_cities c ON l.city_id = c.id
LEFT JOIN fabric_pii.master_states ms ON c.state_id = ms.id
LEFT JOIN fabric_pii.master_countries mcc ON ms.country_id = mcc.id
where mcc.name = 'India'
")
##### PULLING RAW DATA FROM MIXPANEL ----
daterange <- data.frame(start = c("2017-09-01", "2017-10-01", "2017-11-01"), end = c("2017-09-30","2017-10-31", "2017-11-30"), stringsAsFactors = F)

doctor_profile_viewed <- data.frame(source = character(), doctor_id = character(), profile_users = character(), month = as.Date(character()))
doctor_impression <- data.frame(doctor_id = character(), listing_users = character(), month = as.Date(character()))
geo_listing_viewed <- data.frame(city = character(), locality = character(), speciality = character(), listing_users = character(), month = as.Date(character()))
geo_profile_viewed <- data.frame(city = character(), locality = character(), speciality = character(), profile_users = character(), month = as.Date(character()))

system.time(for(i in 2:3){
  jqlQuery <- paste0('function main() {
                     return Events({
                     from_date: "',daterange[i,1],'",
                     to_date: "',daterange[i,2],'",
                     event_selectors: [{event: "Doctor Profile Viewed"}]})
                     .groupBy(["properties.Action Source", "properties.Identification ID"], mixpanel.reducer.count());}')
  
  data <- mixpanelJQLQuery(account, jqlQuery, columnNames = c("source", "doctor_id", "profile_users"))
  data$month <- as.Date(daterange[i,1])
  data <- subset(data, !is.na(data$doctor_id))
  doctor_profile_viewed <- bind_rows(doctor_profile_viewed, data)
  print(daterange[i,1])
  })

doctor_profile_viewed <- doctor_profile_viewed %>%
  filter(!grepl('Practo',source), nchar(doctor_id) <= 10) %>%
  group_by(doctor_id, month) %>%
  summarise(profile_users = sum(as.integer(profile_users))) %>%
  ungroup()
# i=2
# i=3
system.time(for(i in 2:3){
  jqlQuery <- paste0('function main() {
                     return Events({
                     from_date: "',daterange[i,1],'",
                     to_date: "',daterange[i,2],'",
                     event_selectors: [{event: "Doctor Impression"}]})
                     .groupBy(["properties.Identification ID"], mixpanel.reducer.count());}')

  data <- mixpanelJQLQuery(account, jqlQuery, columnNames = c("doctor_id", "listing_users"))
  data$month <- as.Date(daterange[i,1])
  data <- subset(data, !is.na(data$doctor_id))
  doctor_impression <- bind_rows(doctor_impression, data)
  print(daterange[i,1])
  })

# save.image("./Conversion_Analysis_v2.RData")

system.time(for(i in 2:3){
  jqlQuery <- paste0('function main(){
                     return Events({
                     from_date: "',daterange[i,1],'",
                     to_date: "',daterange[i,2],'",
                     event_selectors: [{event: "Doctor Listing Viewed"}]})
                     .groupBy(["properties.City", 
                     "properties.Location Value", 
                     "properties.Query Value"], 
                     mixpanel.reducer.count());}')
  
  data <- mixpanelJQLQuery(account, jqlQuery, columnNames = c("city", "locality", "speciality", "listing_users"))
  data$month <- as.Date(daterange[i,1])
  # data <- subset(data, !is.na(data$doctor_id))
  geo_listing_viewed <- bind_rows(geo_listing_viewed, data)
  print(daterange[i,1])
  })

save.image("./Conversion_Analysis_v3.RData")

system.time(for(i in 2:3){
  jqlQuery <- paste0('function main() {
                     return Events({
                     from_date: "',daterange[i,1],'",
                     to_date: "',daterange[i,2],'",
                     event_selectors: [{event: "Doctor Profile Viewed"}]})
                     .groupBy(["properties.City", 
                     "properties.Location Value", 
                     "properties.Query Value",
                     "properties.Action Source"], 
                     mixpanel.reducer.count());}')
  
  data <- mixpanelJQLQuery(account, jqlQuery, columnNames = c("city", "locality", "speciality", "source", "profile_users"))
  data$month <- as.Date(daterange[i,1])
  # data <- subset(data, !is.na(data$doctor_id))
  geo_profile_viewed <- bind_rows(geo_profile_viewed, data)
  print(daterange[i,1])
  })
geo_profile_viewed <- geo_profile_viewed %>%
  filter(!grepl('Practo',source)) %>%
  group_by(city, locality, speciality, month) %>%
  summarise(profile_users = sum(as.integer(profile_users))) %>%
  ungroup()

save.image("./Conversion_Analysis_v4.RData")

##### DATA CRUNCHING ON FABRIC UNHASHED DATA -----
mixP_spec_cleanup <- read.csv("./mixpanel_db_specialty_map.csv", stringsAsFactors = F)
doc_attributes <- read.csv("./unhashed_doc_attributes_2.csv", stringsAsFactors = F)[,-1]
india_tier <- data.frame(city = c('Bangalore',
                                  'Hyderabad',
                                  'Chennai',
                                  'Pune',
                                  'Kolkata',
                                  'Delhi','Gurgaon','Noida','Ghaziabad','Faridabad',
                                  'Mumbai','Navi Mumbai','Thane'),stringsAsFactors = F)
india_tier$tier <- 'Tier-1'

doc_attributes$month <- as.Date(paste0(doc_attributes$month, "-01"), format = '%Y-%m-%d')

doc_attributes <- doc_attributes %>%
  group_by(doctor_id, month, city, locality, speciality) %>%
  summarise(transactions = sum(total_transactions)) %>%
  ungroup()
doc_attributes$tier <- india_tier$tier[match(doc_attributes$city, india_tier$city)]
doc_attributes <- doc_attributes %>%
  filter(tier == "Tier-1")
###################################################################################################
geo_listing_viewed$tier <- india_tier$tier[match(geo_listing_viewed$city, india_tier$city)]
geo_profile_viewed$tier <- india_tier$tier[match(geo_profile_viewed$city, india_tier$city)]

geo_listing_viewed <- geo_listing_viewed %>%
  filter(tier == "Tier-1") %>%
  group_by(city, locality, speciality, month) %>%
  summarise(listing_users = sum(as.integer(listing_users))) %>%
  ungroup() %>%
  mutate(locality = ifelse(locality == city, locality == "", ifelse(locality == "Near Me", locality == "", ifelse(locality == "Current Location", locality == "", locality))))

geo_profile_viewed <- geo_profile_viewed %>%
  filter(tier == "Tier-1") %>%
  group_by(city, locality, speciality, month) %>%
  summarise(profile_users = sum(as.integer(profile_users))) %>%
  ungroup() %>%
  mutate(locality = ifelse(locality == city, locality == "", ifelse(locality == "Near Me", locality == "", locality)))

# temp <- doctor_listing_viewed %>%
#   group_by(speciality) %>%
#   summarise(users = sum(listing_users)) %>%
#   ungroup() %>%
#   arrange(-users) %>%
#   mutate(cumsum_users = cumsum(users))
# temp$perc <- (temp$cumsum_users/sum(temp$users))*100
# 
# temp$new_spec <- mixP_spec_cleanup$db.specialty[match(temp$speciality, mixP_spec_cleanup$listing.specialty)]
# temp <- temp %>%
#   filter(!is.na(temp$new_spec))

# doctor_listing_viewed$new_spec <- temp$new_spec[match(doctor_listing_viewed$speciality, temp$speciality)]

## Case - 1: When city-loc-spec is present
combo1 <- geo_listing_viewed %>%
  filter(!is.na(locality),
         locality != "",
         speciality != "",
         !is.na(speciality)) %>%
  group_by(city, locality, speciality) %>%
  summarise(listing = sum(listing_users)) %>%
  ungroup()

## Case - 2: When city-loc is present but spec is missing
combo2 <- geo_listing_viewed %>%
  filter(!is.na(locality),
         locality != "",
         is.na(speciality) | speciality == "") %>%
  group_by(city, locality, speciality) %>%
  summarise(listing = sum(listing_users)) %>%
  ungroup()

combo1 <- combo1 %>%
  group_by(city, locality) %>%
  mutate(city_loc_sum = sum(listing)) %>%
  ungroup() %>%
  mutate(ratio_city_loc = listing/city_loc_sum) 

case1 <- merge(combo1, combo2, by = c('city', 'locality'), all.x = T, all.y = T)
case1$case1_listing_users <- case1$listing.y*case1$ratio_city_loc

combo1 <- combo1 %>%
  group_by(city) %>%
  mutate(city_sum = sum(listing)) %>%
  ungroup() %>%
  mutate(ratio_city = listing/city_sum)

## Case - 3: When city is present but loc-spec is missing
combo3 <- geo_listing_viewed %>%
  filter(is.na(locality) | locality == "",
         is.na(speciality) | speciality == "") %>%
  group_by(city, locality, speciality) %>%
  summarise(listing = sum(listing_users)) %>%
  ungroup()
  
case2 <- merge(combo1, combo3, by = 'city', all.x = T, all.y = T)
case2$case2_listing_users <- case2$listing.y*case2$ratio_city

combo1 <- combo1 %>%
  group_by(city, speciality) %>%
  mutate(city_spec_sum = sum(listing)) %>%
  ungroup() %>%
  mutate(ratio_city_spl = listing/city_spec_sum)

## Case - 4: When city-spec is present but loc is missing
combo4 <- geo_listing_viewed %>%
  filter(is.na(locality) | locality == "",
         !is.na(speciality)) %>%
  group_by(city, locality, speciality) %>%
  summarise(listing = sum(listing_users)) %>%
  ungroup()

case3 <- merge(combo1, combo4, by = c('city', 'speciality'), all.x = T, all.y = T)
case3$case3_listing_users <- case3$listing.y*case3$ratio_city_spl

colnames(case1)[3] <- "speciality"
combo1 <- merge(combo1, case1, by = c('city', 'locality', 'speciality'))
combo1[,5:(ncol(combo1)-1)] <- NULL
colnames(case2)[2:3] <- c("locality", "speciality")
combo1 <- merge(combo1, case2, by = c('city', 'locality', 'speciality'))
combo1[,6:(ncol(combo1)-1)] <- NULL
colnames(case3)[3] <- "locality"
combo1 <- merge(combo1, case3, by = c('city', 'locality', 'speciality'))
combo1[,7:(ncol(combo1)-1)] <- NULL

combo1[is.na(combo1)] <- 0

combo1 <- combo1 %>%
  group_by(city, locality, speciality) %>%
  summarise(total_listing_users = sum(listing,case1_listing_users,case2_listing_users,case3_listing_users)) %>%
  ungroup()

##### DATA CRUNCHING ON MIXPANEL DATA -----
doctor_profile_listing_users <- merge(doctor_impression, doctor_profile_viewed, by = c('doctor_id', 'month'), all.x = T, all.y = T)
doctor_profile_listing_users$profile_users <- as.numeric(doctor_profile_listing_users$profile_users)
doctor_profile_listing_users$listing_users <- as.numeric(doctor_profile_listing_users$listing_users)
doctor_profile_listing_users$doctor_id <- as.integer(doctor_profile_listing_users$doctor_id)

doctor_profile_listing_users[is.na(doctor_profile_listing_users)] <- 0
doctor_profile_listing_users <- doctor_profile_listing_users %>%
                                  mutate(users = listing_users + profile_users)

doctor_profile_listing_users <- doctor_profile_listing_users %>%
  group_by(doctor_id, month) %>%
  summarise(listing = sum(listing_users), profile = sum(profile_users), users = sum(users))

################################## DOCTOR-WISE ATTRIBUTES (Doctor_ID-wise) #################################
doctor_city <- doc_attributes %>%
  group_by(doctor_id, month, city) %>%
  summarise(transactions = sum(transactions)) %>%
  ungroup()
doctor_city <- merge(doctor_city, doctor_profile_listing_users, by = c('doctor_id', 'month'), all.x = T, all.y = T)
doctor_city$transactions[is.na(doctor_city$transactions)] <- 0
temp <- unique(doctor_city[,c(1,3)])
temp <- temp%>%filter(!is.na(temp$city))

doctor_city$new_city1 <- temp$city[match(doctor_city$doctor_id, temp$doctor_id)]
doctor_city$new_city2 <- mapping$city[match(doctor_city$doctor_id, mapping$doctor_id)]
doctor_city$new_city <- ifelse(!is.na(doctor_city$new_city1), doctor_city$new_city1, ifelse(!is.na(doctor_city$new_city2), doctor_city$new_city2, "Unknown city"))
doctor_city <- doctor_city %>% filter(new_city != "Unknown city")

doctor_city <- doctor_city %>%
  filter(month == '2017-10-01' | month == '2017-11-01') %>%
  group_by(doctor_id, month, new_city, transactions, users) %>%
  summarise(conversion_percentage = (sum(transactions)/sum(users))*100) %>%
  ungroup() %>%
  filter(!is.na(conversion_percentage))

quantile(doctor_city$conversion_percentage, probs = 0.99)
quantile(doctor_city$conversion_percentage, probs = 0)

doctor_city <- doctor_city %>% filter(conversion_percentage <= quantile(doctor_city$conversion_percentage, probs = 0.99))

doctor_city$tier <- india_tier$tier[match(doctor_city$new_city, india_tier$city)]
doctor_city <- doctor_city %>%
  filter(tier == "Tier-1")

# length(doctor_city$new_city2[is.na(doctor_city$new_city2)])
# length(doctor_city$transactions[is.na(doctor_city$transactions)])

###############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

stats_india <- doctor_city %>%
  summarise(
    conv = (sum(transactions)/ sum(users))*100,
    median_conv = median(conversion_percentage),
    sd_conv = sd(conversion_percentage),
    min_conv = min(conversion_percentage),
    percentile_10 = quantile(doctor_city$conversion_percentage, probs = .1),
    percentile_50 = quantile(doctor_city$conversion_percentage, probs = .5),
    percentile_90 = quantile(doctor_city$conversion_percentage, probs = .9),
    max_conv = max(conversion_percentage)) %>%
  ungroup()
ggplot(data = doctor_city %>% filter(conversion_percentage <= 66.66)
       , aes(x=conversion_percentage)) + 
  geom_density() + 
  scale_x_continuous(breaks = seq(1,100,5)) + 
  scale_y_continuous(breaks = seq(0,1,0.1))

## Bangalore ----
bangalore <- doctor_city %>%
  filter(
    new_city == "Bangalore", 
    conversion_percentage <= 66.66) %>%
  group_by(doctor_id, month) %>%
  summarise(transactions = sum(transactions),
            users = sum(users),
            conversion_perc = (sum(transactions)/sum(users))*100) %>% 
  arrange(month, -conversion_perc) %>%
  ungroup()

length(unique(bangalore$doctor_id))

stats_bangalore <- bangalore %>%
  summarise(
    new_city = "Bangalore",
    mean_conv = (sum(transactions)/ sum(users))*100,
            median_conv = median(conversion_perc),
            sd_conv = sd(conversion_perc),
            min_conv = min(conversion_perc),
            percentile_10 = quantile(bangalore$conversion_perc, probs = .1),
            percentile_90 = quantile(bangalore$conversion_perc, probs = .9),
            max_conv = max(conversion_perc)) %>%
  ungroup()

## Delhi ----
delhi <- doctor_city %>%
  filter(
    new_city == "Delhi", 
    conversion_percentage <= 66.66) %>%
  group_by(doctor_id, month) %>%
  summarise(transactions = sum(transactions),
            users = sum(users),
    conversion_perc = (sum(transactions)/sum(users))*100) %>% 
  arrange(month, -conversion_perc) %>%
  ungroup()

length(unique(delhi$doctor_id))

stats_delhi <- delhi %>%
  summarise(
    new_city = "Delhi",
    mean_conv = (sum(transactions)/ sum(users))*100,
    median_conv = median(conversion_perc),
    sd_conv = sd(conversion_perc),
    min_conv = min(conversion_perc),
    max_conv = max(conversion_perc)) %>%
  ungroup()

## Mumbai ----
mumbai <- doctor_city %>%
  filter(
    # month == '2017-10-01', 
    new_city == "Mumbai", 
    conversion_percentage <= 66.66) %>%
  group_by(doctor_id, month) %>%
  summarise(
    transactions = sum(transactions),
    users = sum(users),
    conversion_perc = (sum(transactions)/sum(users))*100) %>% 
  arrange(month, -conversion_perc) %>%
  ungroup()

stats_mumbai <- mumbai %>%
  summarise(
    new_city = "Mumbai",
    mean_conv = (sum(transactions)/ sum(users))*100,
    median_conv = median(conversion_perc),
    sd_conv = sd(conversion_perc),
    min_conv = min(conversion_perc),
    percentile_10 = quantile(bangalore$conversion_perc, probs = .1),
    percentile_20 = quantile(bangalore$conversion_perc, probs = .2),
    percentile_30 = quantile(bangalore$conversion_perc, probs = .3),
    percentile_40 = quantile(bangalore$conversion_perc, probs = .4),
    percentile_50 = quantile(bangalore$conversion_perc, probs = .5),
    percentile_60 = quantile(bangalore$conversion_perc, probs = .6),
    percentile_70 = quantile(bangalore$conversion_perc, probs = .7),
    percentile_80 = quantile(bangalore$conversion_perc, probs = .8),
    percentile_90 = quantile(bangalore$conversion_perc, probs = .9),
    max_conv = max(conversion_perc)) %>%
  ungroup()
ggplot(data = doctor_city %>% filter(conversion_percentage <= 66.66, new_city %in% c('Bangalore', 'Delhi', 'Mumbai'))
       , aes(x=conversion_percentage)) + 
  geom_density(aes(color = new_city, group = new_city)) + 
  scale_x_continuous(breaks = seq(1,100,5)) +
  scale_y_continuous(breaks = seq(0,1,0.1))

## Pune ----
pune <- doctor_city %>%
  filter(
    # month == '2017-10-01', 
    city == "Pune", 
    conversion_percentage >= 0.27, conversion_percentage <= 72.72) %>%
  group_by(doctor_id, month) %>%
  summarise(
    transactions = sum(transactions),
    users = sum(users),
    conversion_perc = (sum(transactions)/sum(users))*100) %>% 
  arrange(month, -conversion_perc) %>%
  ungroup()

stats_pune <- pune %>%
  summarise(
    city = "Pune",
    mean_conv = (sum(transactions)/ sum(users))*100,
    median_conv = median(conversion_perc),
    sd_conv = sd(conversion_perc),
    min_conv = min(conversion_perc),
    percentile_10 = quantile(bangalore$conversion_perc, probs = .1),
    percentile_20 = quantile(bangalore$conversion_perc, probs = .2),
    percentile_30 = quantile(bangalore$conversion_perc, probs = .3),
    percentile_40 = quantile(bangalore$conversion_perc, probs = .4),
    percentile_50 = quantile(bangalore$conversion_perc, probs = .5),
    percentile_60 = quantile(bangalore$conversion_perc, probs = .6),
    percentile_70 = quantile(bangalore$conversion_perc, probs = .7),
    percentile_80 = quantile(bangalore$conversion_perc, probs = .8),
    percentile_90 = quantile(bangalore$conversion_perc, probs = .9),
    max_conv = max(conversion_perc)) %>%
  ungroup()

setDT(stats_pune)
quantile_pune <- melt(stats_pune)

ggplotly(ggplot() + 
           geom_bar(data = quantile_pune[4:nrow(quantile_pune),], aes(x=variable, y=value), stat = "identity", fill = "deepskyblue4"))

## Hyderabad ----
hyderabad <- doctor_city %>%
  filter(
    # month == '2017-10-01', 
    city == "Hyderabad", 
    conversion_percentage >= 0.27, conversion_percentage <= 72.72) %>%
  group_by(doctor_id, month) %>%
  summarise(
    transactions = sum(transactions),
    users = sum(users),
    conversion_perc = (sum(transactions)/sum(users))*100) %>% 
  arrange(month, -conversion_perc) %>%
  ungroup()

stats_hyderabad <- hyderabad %>%
  summarise(
    city = "Hyderabad",
    mean_conv = (sum(transactions)/ sum(users))*100,
    median_conv = median(conversion_perc),
    sd_conv = sd(conversion_perc),
    min_conv = min(conversion_perc),
    percentile_10 = quantile(bangalore$conversion_perc, probs = .1),
    percentile_20 = quantile(bangalore$conversion_perc, probs = .2),
    percentile_30 = quantile(bangalore$conversion_perc, probs = .3),
    percentile_40 = quantile(bangalore$conversion_perc, probs = .4),
    percentile_50 = quantile(bangalore$conversion_perc, probs = .5),
    percentile_60 = quantile(bangalore$conversion_perc, probs = .6),
    percentile_70 = quantile(bangalore$conversion_perc, probs = .7),
    percentile_80 = quantile(bangalore$conversion_perc, probs = .8),
    percentile_90 = quantile(bangalore$conversion_perc, probs = .9),
    max_conv = max(conversion_perc)) %>%
  ungroup()

setDT(stats_hyderabad)
quantile_hyderabad <- melt(stats_hyderabad)

ggplotly(ggplot() + 
           geom_bar(data = quantile_hyderabad[4:nrow(quantile_hyderabad),], aes(x=variable, y=value), stat = "identity", fill = "deepskyblue4"))





## Chennai ----
chennai <- doctor_city %>%
  filter(
    # month == '2017-10-01', 
    city == "Chennai", 
    conversion_percentage >= 0.27, conversion_percentage <= 72.72) %>%
  group_by(doctor_id, month) %>%
  summarise(
    transactions = sum(transactions),
    users = sum(users),
    conversion_perc = (sum(transactions)/sum(users))*100) %>% 
  arrange(month, -conversion_perc) %>%
  ungroup()

stats_chennai <- chennai %>%
  summarise(
    city = "Chennai",
    mean_conv = (sum(transactions)/ sum(users))*100,
    median_conv = median(conversion_perc),
    sd_conv = sd(conversion_perc),
    min_conv = min(conversion_perc),
    percentile_10 = quantile(bangalore$conversion_perc, probs = .1),
    percentile_20 = quantile(bangalore$conversion_perc, probs = .2),
    percentile_30 = quantile(bangalore$conversion_perc, probs = .3),
    percentile_40 = quantile(bangalore$conversion_perc, probs = .4),
    percentile_50 = quantile(bangalore$conversion_perc, probs = .5),
    percentile_60 = quantile(bangalore$conversion_perc, probs = .6),
    percentile_70 = quantile(bangalore$conversion_perc, probs = .7),
    percentile_80 = quantile(bangalore$conversion_perc, probs = .8),
    percentile_90 = quantile(bangalore$conversion_perc, probs = .9),
    max_conv = max(conversion_perc)) %>%
  ungroup()

setDT(stats_chennai)
quantile_chennai <- melt(stats_chennai)

ggplotly(ggplot() + 
           geom_bar(data = quantile_chennai[4:nrow(quantile_chennai),], aes(x=variable, y=value), stat = "identity", fill = "deepskyblue4"))






## Kolkata ----
kolkata <- doctor_city %>%
  filter(
    # month == '2017-10-01', 
    city == "Kolkata", 
    conversion_percentage >= 0.27, conversion_percentage <= 72.72) %>%
  group_by(doctor_id, month) %>%
  summarise(
    transactions = sum(transactions),
    users = sum(users),
    conversion_perc = (sum(transactions)/sum(users))*100) %>% 
  arrange(month, -conversion_perc) %>%
  ungroup()

stats_kolkata <- kolkata %>%
  summarise(
    city = "Kolkata",
    mean_conv = (sum(transactions)/ sum(users))*100,
    median_conv = median(conversion_perc),
    sd_conv = sd(conversion_perc),
    min_conv = min(conversion_perc),
    percentile_10 = quantile(bangalore$conversion_perc, probs = .1),
    percentile_20 = quantile(bangalore$conversion_perc, probs = .2),
    percentile_30 = quantile(bangalore$conversion_perc, probs = .3),
    percentile_40 = quantile(bangalore$conversion_perc, probs = .4),
    percentile_50 = quantile(bangalore$conversion_perc, probs = .5),
    percentile_60 = quantile(bangalore$conversion_perc, probs = .6),
    percentile_70 = quantile(bangalore$conversion_perc, probs = .7),
    percentile_80 = quantile(bangalore$conversion_perc, probs = .8),
    percentile_90 = quantile(bangalore$conversion_perc, probs = .9),
    max_conv = max(conversion_perc)) %>%
  ungroup()

stats <- rbind(stats_kolkata, stats_chennai, stats_hyderabad, stats_pune, stats_mumbai, stats_delhi, stats_bangalore)

temp <- doctor_city %>% filter(month == "2017-10-01", conversion_percentage <= 72.72)
length(unique(temp$doctor_id))

ggplot(data = doctor_city %>% filter(conversion_percentage >= 0.27, conversion_percentage <= 72.72, city %in% c('Pune', 'Hyderabad'))
       , aes(x=conversion_percentage)) + 
  geom_density(aes(color = city, group = city)) + 
  scale_x_continuous(breaks = seq(1,100,5))

ggplot(data = doctor_city %>% filter(conversion_percentage >= 0.27, conversion_percentage <= 72.72, city %in% c('Chennai', 'Kolkata'))
       , aes(x=conversion_percentage)) + 
  geom_density(aes(color = city, group = city)) + 
  scale_x_continuous(breaks = seq(1,100,5))

ggplot(data = doctor_city %>% filter(conversion_percentage <= 72.72)
       , aes(x=conversion_percentage)) + 
  geom_density(fill = "deepskyblue4") + 
  scale_x_continuous(breaks = seq(1,100,5))

stats[,8:10] <- NULL

write.csv(stats, "./stats_India.csv")

################################## GEOGRAPHY-WISE ATTRIBUTES (Locality-Speciality) ###########################

## Attribute - 01 - SPECIALITY -----
doctor_speciality <- doc_attributes %>%
  group_by(doctor_id, month, speciality) %>%
  summarise(transaction = sum(transactions)) %>%
  ungroup()
doctor_speciality <- merge(doctor_speciality, doctor_profile_listing_users, by = c('doctor_id', 'month'), all.x = T, all.y = T)
doctor_speciality <- doctor_speciality %>% 
  filter(month == '2017-10-01' | month == '2017-11-01') %>%
  filter(!is.na(speciality)) %>%
  group_by(doctor_id, month, speciality, transaction, users) %>%
  summarise(conversion_percentage = (sum(transaction)/sum(users))*100) %>%
  filter(!is.na(conversion_percentage)) %>%
  ungroup()

quantile(doctor_speciality$conversion_percentage, probs = 0.98)
quantile(doctor_speciality$conversion_percentage, probs = 0.02)

data1 <- doctor_speciality %>% 
filter(month == '2017-10-01', conversion_percentage >= 0.23, conversion_percentage <= 25) %>% 
group_by(speciality) %>% 
summarise(trans = sum(transaction), users = sum(users), conversion_perc = (sum(transaction)/sum(users))*100) %>% ungroup()

data2 <- doctor_speciality %>% 
  filter(month == '2017-11-01', conversion_percentage >= 0.23, conversion_percentage <= 25) %>% 
  group_by(speciality) %>% 
  summarise(trans = sum(transaction), 
            users = sum(users), 
            conversion_perc = (sum(transaction)/sum(users))*100) %>% 
  ungroup()

## Attribute - 02 - EXPERIENCE ----
doctor_experience <- doc_attributes %>%
  group_by(doctor_id, month, experience) %>%
  summarise(transaction = sum(transactions)) %>%
  ungroup()
doctor_experience <- merge(doctor_experience, doctor_profile_listing_users, by = c('doctor_id', 'month'), all.x = T, all.y = T)
doctor_experience <- doctor_experience %>%
  filter(month == '2017-10-01' | month == '2017-11-01') %>%
  filter(!is.na(experience)) %>%
  group_by(doctor_id, month, experience, transaction, users) %>%
  summarise(conversion_percentage = (sum(transaction)/sum(users))*100) %>%
  filter(!is.na(conversion_percentage)) %>%
  ungroup()

quantile(doctor_experience$conversion_percentage, probs = 0.98)
quantile(doctor_experience$conversion_percentage, probs = 0.02)

data3 <- doctor_experience %>% 
  mutate(experience_buckets = ifelse(experience <= 5, "0-5", ifelse(experience <= 10, "6-10", ifelse(experience <= 15, "11-15", ifelse(experience <= 20, "16-20", ifelse(experience <= 30, "21-30", ifelse(experience <= 50, "31-50", "junk"))))))) %>%
  filter(conversion_percentage >= 0.30, conversion_percentage <= 29.96, experience_buckets != "junk") %>% 
  group_by(experience_buckets) %>% 
  summarise(trans = sum(transaction), users = sum(users), conversion_perc = (sum(transaction)/sum(users))*100) %>% ungroup()

## Attribute - 03
doctor_city <- doc_attributes %>%
  group_by(doctor_id, month, city) %>%
  summarise(transaction = sum(transactions)) %>%
  ungroup()
doctor_city <- merge(doctor_city, doctor_profile_listing_users, by = c('doctor_id', 'month'), all.x = T, all.y = T)
doctor_city <- doctor_city %>%
  filter(month == '2017-10-01' | month == '2017-11-01') %>%
  filter(!is.na(city)) %>%
  group_by(doctor_id, month, city, transaction, users) %>%
  summarise(conversion_percentage = (sum(transaction)/sum(users))*100) %>%
  filter(!is.na(conversion_percentage)) %>%
  ungroup()

quantile(doctor_city$conversion_percentage, probs = 0.98)
quantile(doctor_city$conversion_percentage, probs = 0.02)

data4 <- doctor_city %>% 
  filter(month == '2017-10-01', conversion_percentage >= 0.23, conversion_percentage <= 37.5) %>% 
  group_by(city) %>% 
  summarise(trans = sum(transaction), users = sum(users), conversion_perc = (sum(transaction)/sum(users))*100) %>% ungroup()

data5 <- doctor_city %>% 
  filter(month == '2017-11-01', conversion_percentage >= 0.23, conversion_percentage <= 37.5) %>% 
  group_by(city) %>% 
  summarise(trans = sum(transaction), 
            users = sum(users), 
            conversion_perc = (sum(transaction)/sum(users))*100) %>% 
  ungroup()

## Attribute - 04
doctor_locality <- doc_attributes %>%
  group_by(doctor_id, month, city, locality) %>%
  summarise(transaction = sum(transactions)) %>%
  ungroup()
doctor_locality <- merge(doctor_locality, doctor_profile_listing_users, by = c('doctor_id', 'month'), all.x = T, all.y = T)
doctor_locality <- doctor_locality %>%
  filter(month == '2017-10-01' | month == '2017-11-01') %>%
  filter(!is.na(locality)) %>%
  group_by(doctor_id, month, city, locality, transaction, users) %>%
  summarise(conversion_percentage = (sum(transaction)/sum(users))*100) %>%
  filter(!is.na(conversion_percentage)) %>%
  ungroup()

quantile(doctor_locality$conversion_percentage, probs = 0.98)
quantile(doctor_locality$conversion_percentage, probs = 0.02)

data6 <- doctor_locality %>% 
  filter(month == '2017-10-01', conversion_percentage >= 0.11, conversion_percentage <= 33.09) %>% 
  group_by(locality, city) %>% 
  summarise(trans = sum(transaction), users = sum(users), conversion_perc = (sum(transaction)/sum(users))*100) %>% ungroup()

data7 <- doctor_locality %>% 
  filter(month == '2017-11-01', conversion_percentage >= 0.11, conversion_percentage <= 33.09) %>% 
  group_by(locality, city) %>% 
  summarise(trans = sum(transaction), 
            users = sum(users), 
            conversion_perc = (sum(transaction)/sum(users))*100) %>% 
  ungroup()

## Attribute - 05
doctor_rank <- doc_attributes %>%
  group_by(doctor_id, month, city, locality) %>%
  summarise(transaction = sum(transactions)) %>%
  ungroup()
doctor_locality <- merge(doctor_locality, doctor_profile_listing_users, by = c('doctor_id', 'month'), all.x = T, all.y = T)


## Attribute - 06
## Attribute - 07
## Attribute - 08
## Attribute - 09
## Attribute - 10

## Copy data to Google Sheets ----
gs_auth(token = "./googlesheets_token.rds")
gs_object <- gs_key('14fqdHLlky1Ke-iARYGfDCZwYarIy2ebzUwshSArrcZs')

gs_edit_cells(gs_object, ws='Data', input = data1 %>% filter(users>= 1000) %>% arrange(-conversion_perc), byrow=TRUE, anchor="A1")
gs_edit_cells(gs_object, ws='Data', input = data2 %>% filter(users>= 1000) %>% arrange(-conversion_perc), anchor="F1", col_names=T, trim=FALSE)
gs_edit_cells(gs_object, ws='Data', input = data3 %>% filter(users>= 1000) %>% arrange(-conversion_perc), byrow=TRUE, anchor="K1")
gs_edit_cells(gs_object, ws='Data', input = data4 %>% filter(users>= 1000) %>% arrange(-conversion_perc), byrow=TRUE, anchor="P1")
gs_edit_cells(gs_object, ws='Data', input = data5 %>% filter(users>= 1000) %>% arrange(-conversion_perc), byrow=TRUE, anchor="U1")
# gs_edit_cells(gs_object, ws='Data', input = data6 %>% filter(users>= 1000) %>% arrange(-conversion_perc), byrow=TRUE, anchor="Z1")
# gs_edit_cells(gs_object, ws='Data', input = data7 %>% filter(users>= 1000) %>% arrange(-conversion_perc), byrow=TRUE, anchor="AE1")

## Plots ----

ggplotly(ggplot() + 
  geom_bar(data = data1 %>% filter(users>= 1000) %>% arrange(-conversion_perc), aes(x=speciality, y=conversion_perc), stat = "identity")) 

ggplotly(ggplot() + 
           geom_bar(data = data2 %>% filter(users>= 1000) %>% arrange(-conversion_perc), aes(x=speciality, y=conversion_perc), stat = "identity")) 

ggplotly(ggplot() + 
           geom_bar(data = data3 %>% filter(users>= 1000) %>% arrange(-conversion_perc), aes(x=experience_buckets, y=conversion_perc), stat = "identity")) 

ggplotly(ggplot() + 
           geom_bar(data = data4 %>% filter(users>= 1000) %>% arrange(-conversion_perc), aes(x=city, y=conversion_perc), stat = "identity"))

#########################################################################################################
conversion <- merge(dplu_summary, doc_attributes, by.all = c("doctor_id", "month"), all.x = T, all.y = T)
conversion <- subset(conversion, nchar(conversion$doctor_id) <= 10 & conversion$month >= '2017-10-01' & conversion$month <= '2017-11-30' & !is.na(conversion$users) & !is.na(conversion$transactions) & !is.na(conversion$experience))
conversion$conversion_perc <- (conversion$transactions/conversion$users)*100
conversion <- merge(conversion, doctor_rank, by = c('doctor_id', 'month'), all.x = T)

doctor_conversion_3months <- conversion %>%
  group_by(doctor_id) %>%
  summarise(users = sum(users), 
  transactions = sum(transactions),
  conversion_percentage = (sum(transactions)/sum(users))*100) %>%
  ungroup()
  
doctor_conversion_3months$city <- conversion$city[match(doctor_conversion_3months$doctor_id, conversion$doctor_id )]  
doctor_conversion_3months$locality <- conversion$locality[match(doctor_conversion_3months$doctor_id, conversion$doctor_id )]  
doctor_conversion_3months$experience <- conversion$experience[match(doctor_conversion_3months$doctor_id, conversion$doctor_id )]  

doctor_conversion_3months <- subset(doctor_conversion_3months, !is.na(doctor_conversion_3months$experience))

save.image("./Conversion_Analysis.RData")
