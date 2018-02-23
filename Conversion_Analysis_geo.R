## Case - 1: When city-loc-spec is present
p_combo1 <- geo_profile_viewed %>%
  filter(!is.na(locality),
         locality != "",
         speciality != "",
         !is.na(speciality)) %>%
  group_by(city, locality, speciality) %>%
  summarise(profile = sum(profile_users)) %>%
  ungroup()

## Case - 2: When city-loc is present but spec is missing
p_combo2 <- geo_profile_viewed %>%
  filter(!is.na(locality),
         locality != "",
         is.na(speciality) | speciality == "") %>%
  group_by(city, locality, speciality) %>%
  summarise(profile = sum(profile_users)) %>%
  ungroup()

p_combo1 <- p_combo1 %>%
  group_by(city, locality) %>%
  mutate(city_loc_sum = sum(profile)) %>%
  ungroup() %>%
  mutate(ratio_city_loc = profile/city_loc_sum) 

case1 <- merge(p_combo1, p_combo2, by = c('city', 'locality'), all.x = T, all.y = T)
case1$case1_profile_users <- case1$profile.y*case1$ratio_city_loc

p_combo1 <- p_combo1 %>%
  group_by(city) %>%
  mutate(city_sum = sum(profile)) %>%
  ungroup() %>%
  mutate(ratio_city = profile/city_sum)

## Case - 3: When city is present but loc-spec is missing
p_combo3 <- geo_profile_viewed %>%
  filter(is.na(locality) | locality == "",
         is.na(speciality) | speciality == "") %>%
  group_by(city, locality, speciality) %>%
  summarise(profile = sum(profile_users)) %>%
  ungroup()

case2 <- merge(p_combo1, p_combo3, by = 'city', all.x = T, all.y = T)
case2$case2_profile_users <- case2$profile.y*case2$ratio_city

p_combo1 <- p_combo1 %>%
  group_by(city, speciality) %>%
  mutate(city_spec_sum = sum(profile)) %>%
  ungroup() %>%
  mutate(ratio_city_spl = profile/city_spec_sum)

## Case - 4: When city-spec is present but loc is missing
p_combo4 <- geo_profile_viewed %>%
  filter(is.na(locality) | locality == "",
         !is.na(speciality)) %>%
  group_by(city, locality, speciality) %>%
  summarise(profile = sum(profile_users)) %>%
  ungroup()

case3 <- merge(p_combo1, p_combo4, by = c('city', 'speciality'), all.x = T, all.y = T)
case3$case3_profile_users <- case3$profile.y*case3$ratio_city_spl

colnames(case1)[3] <- "speciality"
p_combo1 <- merge(p_combo1, case1, by = c('city', 'locality', 'speciality'))
p_combo1[,5:(ncol(p_combo1)-1)] <- NULL
colnames(case2)[2:3] <- c("locality", "speciality")
p_combo1 <- merge(p_combo1, case2, by = c('city', 'locality', 'speciality'))
p_combo1[,6:(ncol(p_combo1)-1)] <- NULL
colnames(case3)[3] <- "locality"
p_combo1 <- merge(p_combo1, case3, by = c('city', 'locality', 'speciality'))
p_combo1[,7:(ncol(p_combo1)-1)] <- NULL

p_combo1[is.na(p_combo1)] <- 0

p_combo1 <- p_combo1 %>%
  group_by(city, locality, speciality) %>%
  summarise(total_profile_users = sum(profile,case1_profile_users,case2_profile_users,case3_profile_users)) %>%
  ungroup()

###########################################################################################################

geo_users <- merge(combo1, p_combo1, by = c('city', 'locality', 'speciality'), all.x = T, all.y = T)
geo_users[is.na(geo_users)] <- 0

geo_users <- geo_users %>% mutate(users = total_listing_users + total_profile_users)
geo_users$new_spec <- mixP_spec_cleanup$db.specialty[match(geo_users$speciality, mixP_spec_cleanup$listing.specialty)]
geo_users <- geo_users %>% filter(!is.na(new_spec)) %>%
  group_by(city, new_spec, locality) %>%
  summarise(total_listing_users = sum(total_listing_users),
            total_profile_users = sum(total_profile_users), 
            users = sum(users)) %>%
  ungroup()
geo_users$speciality <- geo_users$new_spec
geo_users$new_spec <- NULL

## Preprocessing transactions data for city-level conversion ----
transactions <- dbGetQuery(conn2, "
select 
Month, 
City, 
total_transactions

FROM
(
(select practice_doctor_id, 
doctor_id,
Month, 
City, 
Locality, 
speciality, 
sum(Tranx) total_transactions
from
(
(SELECT pd.id as practice_doctor_id,
dd.id as doctor_id,
left(a.created_at,7) as Month, 
c.name as City,
l.name as Locality,
mds.speciality as Speciality, 
COUNT(DISTINCT a.id) as Tranx
FROM
fabric_pii.appointments a
LEFT JOIN fabric_pii.practice_doctors_published pd ON a.practiceDoctor_id = pd.id
LEFT JOIN fabric_pii.doctors_published dd ON dd.id = pd.doctor_id 
LEFT JOIN fabric_pii.doctor_specializations_published ds ON dd.id = ds.doctor_id
LEFT JOIN fabric_pii.master_doctor_subspecialities mdss ON ds.subspecialization_id = mdss.id
LEFT JOIN fabric_pii.master_doctor_specialities mds ON  mds.id = mdss.speciality_id
LEFT JOIN fabric_pii.practices_published p ON p.id = pd.practice_id
LEFT JOIN fabric_pii.master_localities l ON p.locality_id = l.id
LEFT JOIN fabric_pii.master_cities c ON l.city_id = c.id
WHERE a.STATUS IN ('CONFIRMED', 'CANCELLED')
AND a.type IN ('ABS', 'abs')
AND a.source NOT IN ('tab', 'widget')
AND a.created_at >= '2017-10-01'
GROUP BY 1,2,3,4,5,6
ORDER BY 1)
UNION
(SELECT pd.id as practice_doctor_id, 
dd.id as doctor_id,
LEFT(vn.created_at,7) as Month, 
c.name as City,
l.name as Locality,
mds.speciality as Speciality,
COUNT(DISTINCT vn.id) as Tranx
FROM fabric_pii.vn_calls vn
JOIN fabric_pii.vn_call_forwardings vncf on vncf.vn_call_id = vn.id
LEFT JOIN fabric_pii.vn_practices vp ON vp.id = vn.vn_practice_id
LEFT JOIN fabric_pii.practice_doctors_published pd ON vp.practice_doctor_id = pd.id
LEFT JOIN fabric_pii.doctors_published dd ON dd.id = pd.doctor_id 
LEFT JOIN fabric_pii.doctor_specializations_published ds ON dd.id = ds.doctor_id
LEFT JOIN fabric_pii.master_doctor_subspecialities mdss ON ds.subspecialization_id = mdss.id
LEFT JOIN fabric_pii.master_doctor_specialities mds ON  mds.id = mdss.speciality_id
LEFT JOIN fabric_pii.practices_published p ON p.id = pd.practice_id
LEFT JOIN fabric_pii.master_localities l ON p.locality_id = l.id
LEFT JOIN fabric_pii.master_cities c ON l.city_id = c.id
LEFT JOIN fabric_pii.master_states ms ON c.state_id = ms.id
LEFT JOIN fabric_pii.master_countries mcc ON ms.country_id = mcc.id
WHERE vn.created_at >= '2017-10-01'
GROUP BY 1,2,3,4,5,6
ORDER BY 1))
GROUP by 1,2,3,4,5,6
order by 1,3,4))")

transactions <- transactions %>% 
  filter(month == "2017-10" | month == "2017-11") %>%
  group_by(city) %>%
  summarise(transactions = sum(total_transactions)) %>%
  ungroup()

doc_attributes_city <- doc_attributes
doc_attributes_city$speciality <- doc_attributes_city$locality <- NULL
doc_attributes_city <- unique(doc_attributes_city)

doc_attributes_city <- doc_attributes %>% 
  filter(month == "2017-10-01" | month == "2017-11-01") %>%
  group_by(city, locality) %>%
  summarise(transactions = sum(transactions)) %>%
  ungroup()
  
geo_users_city <- geo_users
geo_users_city$speciality <- geo_users_city$total_listing_users <- geo_users_city$total_profile_users <- NULL
geo_users_city <- unique(geo_users_city)
geo_users_city <- geo_users_city %>% group_by(city) %>% summarise(users = sum(users))

geo_users_trans_city <- merge(geo_users_city, transactions, by = c('city'), all.x = T, all.y = T)
geo_users_trans_city$transactions[is.na(geo_users_trans_city$transactions)] <- 0
geo_users_trans_city <- geo_users_trans_city %>% filter(!is.na(geo_users_trans_city$users))
geo_users_trans_city$conv <- (geo_users_trans_city$transactions/geo_users_trans_city$users)*100
geo_users_trans_city <- geo_users_trans_city %>%
  mutate(city = ifelse((city == 'Mumbai' |
                          city == 'Navi Mumbai'|
                          city == 'Thane'), 'Mumbai_MMR',
                       ifelse((city == 'New Delhi' |
                                 city == 'Delhi' |
                                 city == 'Ghaziabad' |
                                 city == 'Faridabad' |
                                 city == 'Noida' |
                                 city == 'Greater Noida' |
                                 city == 'Gurgaon'), 'Delhi_NCR',
                              ifelse((city == 'Bangalore' |
                                        city == 'Bangalore Rural'), 'Bengaluru',
                                     ifelse(city == 'S??o Paulo', 'Sao Paulo', city))))) %>%
  group_by(city) %>%
  summarise(transactions = sum(transactions),
            users = sum(users),
            conv = (transactions/users)*100) %>%
  ungroup()

temp <- geo_users_trans_city %>%
  group_by(city) %>%
  summarise(
    # listing = sum(total_listing_users),
            # profile = sum(total_profile_users), 
            users = sum(users),
            transactions = sum(transactions),
            conversion_perc = (transactions/users)*100) %>%
  ungroup()

quantile(geo_users_trans$conversion_perc, probs = 0.96)

geo_users_trans <- geo_users_trans %>% filter(conversion_perc <= quantile(geo_users_trans$conversion_perc, probs = 0.96))

## Merging with doc_attributes for transactions data ----
doc_attributes <- doc_attributes %>% 
  filter(month == "2017-10-01" | month == "2017-11-01") %>%
  group_by(city, locality, speciality) %>%
  summarise(transactions = sum(transactions)) %>%
  ungroup()

geo_users_trans <- merge(geo_users, doc_attributes, by = c('city', 'locality', 'speciality'), all.x = T, all.y = T)
geo_users_trans$transactions[is.na(geo_users_trans$transactions)] <- 0
geo_users_trans <- geo_users_trans %>%
  group_by(city, locality, speciality) %>%
  summarise(listing = sum(total_listing_users),
            profile = sum(total_profile_users), 
            users = sum(users),
            transactions = sum(transactions),
            conversion_perc = (transactions/users)*100) %>%
  ungroup() %>%
  filter(!is.na(conversion_perc))

quantile(geo_users_trans$conversion_perc, probs = 0.96)

geo_users_trans <- geo_users_trans %>% filter(conversion_perc <= quantile(geo_users_trans$conversion_perc, probs = 0.96))

geo_users_trans_city_spec <- geo_users_trans %>%
  group_by(city, speciality) %>%
  summarise(listing = sum(listing),
            profile = sum(profile), 
            users = sum(users),
            transactions = sum(transactions),
            conversion_perc = (transactions/users)*100) %>%
  ungroup() %>%
  filter(!is.na(conversion_perc))

geo_users_trans_loc_spec <- geo_users_trans %>%
  group_by(city, locality, speciality) %>%
  summarise(listing = sum(listing),
            profile = sum(profile), 
            users = sum(users),
            transactions = sum(transactions),
            conversion_perc = (transactions/users)*100) %>%
  ungroup() %>%
  filter(!is.na(conversion_perc))

quantile(geo_users_trans_loc_spec$transactions, probs = 0.98)

## Out of 103k combinations, only 2% have more than 
geo_users_trans_loc_spec <- geo_users_trans_loc_spec %>% filter(transactions > 121)

################
geo_users_trans_india <- geo_users_trans %>%
  group_by(speciality) %>%
  summarise(listing = sum(listing),
            profile = sum(profile), 
            users = sum(users),
            transactions = sum(transactions),
            conversion_perc = (transactions/users)*100) %>%
  ungroup() %>%
  filter(!is.na(conversion_perc)) %>%
  arrange(-transactions)

quantile(geo_users_trans_india$transactions, probs = 0.2)

geo_users_trans_india <- geo_users_trans_india %>% filter(transactions >= 350)

geo_users_trans_city_spec$top_specs <- geo_users_trans_india$speciality[match(geo_users_trans_city_spec$speciality, geo_users_trans_india$speciality)]
################
geo_users_trans_city_spec <- geo_users_trans_city_spec %>%
  filter(!is.na(top_specs)) %>%
  group_by(city, top_specs) %>%
  summarise(users = sum(users),
            transactions = sum(transactions),
            conversion_perc = (transactions/users)*100) %>%
  ungroup()

ggplot(data = geo_users_trans_city_spec, aes(x=top_specs, y=conversion_perc)) +
  geom_line(aes(color = city, group = city))
  
ggplot(data = geo_users_trans_city_spec, aes(x=conversion_perc)) +
  geom_density() + 
  scale_x_continuous(breaks = seq(1,100,5)) + 
  scale_y_continuous(breaks = seq(0,1,0.1))

ggplot(data = geo_users_trans_city_spec, aes(x=conversion_perc)) +
  geom_histogram(binwidth = 1)

write.csv(geo_users_trans_city_spec, "./geo_users_trans_city_spec.csv")

save.image("./conv.RData")
