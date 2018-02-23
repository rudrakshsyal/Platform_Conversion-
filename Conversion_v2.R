# Aim to analyze conversion% from different dimensions such as -
# 1. speciality
# 2. locality
# 3. platform
# 4. mode
# 5. doctor-level attributes (consultation_fee, reviews, recommendations, etc.)

##### 1. LOADING LIBRARIES -----

suppressMessages(source("./load_libraries.R"))
library(RMixpanel)
library(binr)

##### 2. SETTING UP CONNECTION TO THE DATABASE ----
source("./DbCreds.R")
##### 3. CREATION OF MIXPANEL ACCOUNT ----
##### 4. LIVE DOCTORS QUERY ----
live_doctors <- dbGetQuery(conn2, "select 
distinct d.id as doctor_id
from fabric_pii.practices_published p  
inner join fabric_pii.practice_doctors_published pd on pd.practice_id=p.id  
inner join fabric_pii.doctors_published d on d.id=pd.doctor_id  
inner join fabric_pii.doctor_verification_statuses_published dv on dv.doctor_id = d.id  
inner join fabric_pii.master_localities ml on ml.id= p.locality_id  
inner join fabric_pii.master_cities mc on mc.id = ml.city_id
inner join fabric_pii.master_states ms on ms.id = mc.state_id
inner join fabric_pii.master_countries mcc on mcc.id = ms.country_id
left join fabric_pii.practice_types_published pt on (pt.practice_id=p.id and pt.deleted_at is null)
where p.deleted_at is null
and pd.deleted_at is null 
and dv.deleted_at is null 
and mcc.name = 'India'
--AND mc.name IN ('Bangalore', 'Hyderabad', 'Mumbai', 'Thane', 'Delhi', 'Gurgaon', 'Chennai', 'Pune', 'Navi Mumbai', 'Noida', 'Kolkata', 'Faridabad', 'Ghaziabad', 'Noida')
and pd.profile_published =1 
and ml.deleted_at is null 
and dv.verification_status = 'VERIFIED' 
and p.demo = 0")
live_doctors_process <- live_doctors
# length(unique(live_doctors$doctor_id))
##### 5. TRANSACTIONS [ABS + VN_FORWARD + QIKWELL]----
trans <- dbGetQuery(conn2, "
select practice_doctor_id, 
doctor_id,
Month, 
sum(Tranx) total_transactions
from
((SELECT pd.id as practice_doctor_id,
dd.id as doctor_id,
left(a.created_at,7) as Month, 
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
AND a.created_at < '2017-12-01'
GROUP BY 1,2,3
ORDER BY 1)
UNION
(SELECT pd.id as practice_doctor_id, 
dd.id as doctor_id,
LEFT(vn.created_at,7) as Month,
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
and vn.created_at < '2017-12-01'
and mcc.name = 'India'
GROUP BY 1,2,3
ORDER BY 1)
UNION
(SELECT
pd.id as practice_doctor_id,
dp.id as doctor_id,
LEFT(am.created_at,7) as Month,
COUNT(DISTINCT am.id) as Tranx
FROM fabric_pii.appointment_mapping am
JOIN fabric_pii.practice_doctors_published pd ON am.practice_doctor_id = pd.id
JOIN fabric_pii.doctors_published dp ON dp.id = pd.doctor_id
JOIN fabric_pii.practices_published p ON p.id = pd.practice_id
JOIN fabric_pii.patients pt ON pt.id = am.patient_id
WHERE am.created_at >= '2017-10-01'
AND am.created_at < '2017-12-01'
GROUP BY 1,2,3))
GROUP by 1,2,3
order by 1,2,3")
length(unique(trans$doctor_id))
trans$total_transactions[trans$doctor_id == '268068' & trans$month == '2017-11']
trans_process <- trans %>% 
  group_by(doctor_id) %>% 
  summarise(transactions = sum(total_transactions)) %>% 
  ungroup()
trans_process$transactions[trans_process$doctor_id == '268068']
live_doctors_process <- merge(live_doctors_process, trans_process, by = 'doctor_id', all.x = T)
live_doctors_process$transactions[is.na(live_doctors_process$transactions)] <- 0
##### 6. USERS - MIXPANEL [DOCTOR IMPRESSION + (DOCTOR PROFILE VIEWED - PRACTO)] ----
load("./conv.RData")
doctor_profile_listing_users_process <- doctor_profile_listing_users
doctor_profile_listing_users_process <- doctor_profile_listing_users_process %>%
  group_by(doctor_id) %>%
  summarise(listing = sum(listing),
            profile = sum(profile),
            users = sum(users)) %>%
  ungroup()
live_doctors_process <- merge(live_doctors_process, doctor_profile_listing_users_process, by = 'doctor_id', all.x = T)
# live_doctors_process$doctor_id[is.na(live_doctors_process$users)]
live_doctors_process <- live_doctors_process %>% filter(!is.na(users))
live_doctors_process$users[live_doctors_process$doctor_id == '268068']
live_doctors_process$listing[live_doctors_process$doctor_id == '268068']
live_doctors_process$profile[live_doctors_process$doctor_id == '268068']
live_doctors_process$listing <- live_doctors_process$profile <- NULL
live_doctors_process$conversion <- (live_doctors_process$transactions/live_doctors_process$users)*100
length(unique(live_doctors_process$doctor_id))
##### 7. DOCTOR ATTRIBUTE - QUALIFICATION ----
qual <- dbGetQuery(conn2,
"select 
pdp.id as practice_doctor_id,
pdp.doctor_id as doctor_id,
count(dq.qualification_id) as qualifications 
from fabric_pii.doctor_qualifications_published dq
join fabric_pii.practice_doctors_published pdp on pdp.doctor_id = dq.doctor_id
join fabric_pii.practices_published pp on pp.id = pdp.practice_id
where pdp.deleted_at is NULL 
and pdp.profile_published = 1 
and dq.published = 1 
and dq.deleted_at is NULL
group by 1,2
order by 1,2")
qual_process <- unique(qual[,2:3])
qual_process$qualifications[qual_process$doctor_id == '268068']
live_doctors_process <- merge(live_doctors_process, qual_process, by = 'doctor_id', all.x = T)
live_doctors_process$qualifications[live_doctors_process$doctor_id == '268068']
length(live_doctors_process$qualifications[is.na(live_doctors_process$qualifications)])

## 114,208 live doctors - INDIA
dq <- live_doctors_process[,c(2,3,5)]
dq <- dq %>% filter(!is.na(qualifications))
## 89,396 live doctors - INDIA
quantile(dq$qualifications, probs = .9995, na.rm = T)
# dq <- dq %>% filter(qualifications <= 9)
dq$conversion <- (dq$transactions/dq$users)*100
quantile(dq$conversion, probs = .99, na.rm = T)
dq <- dq %>% filter(conversion <= 100)
## 88,818 live doctors - INDIA
summary(dq)

dq <- dq %>% 
  mutate(buckets = ifelse(qualifications == 0, "not_qualified", ifelse(qualifications == 1, "less_qualified", ifelse(qualifications == 2, "average_qualified", "more_qualified")))) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

dq_buckets <- dq %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = dq[,3:4], aes(x=qualifications)) + geom_density()

ggplot(dq, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = dq_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = dq[,3:4], aes(x=qualifications, y=conversion)) + geom_point(size = 0.5, color = "red")
##### 8. DOCTOR ATTRIBUTE - EXPERIENCE ----
exp <- dbGetQuery(conn2,
"select 
pdp.id as practice_doctor_id,
pdp.doctor_id as doctor_id,
(2018 - dp.practicing_start_year) as experience
from fabric_pii.doctors_published dp 
join fabric_pii.practice_doctors_published pdp on pdp.doctor_id=dp.id
join fabric_pii.practices_published pp on pdp.practice_id=pp.id
where dp.deleted_at is null 
and pdp.profile_published=1 
and dp.dqs is not null
and dp.practicing_start_year is not null")

exp_process <- unique(exp[,2:3])
exp_process$experience[exp_process$doctor_id == '268068']
live_doctors_process <- merge(live_doctors_process, exp_process, by = 'doctor_id', all.x = T)
live_doctors_process$experience[live_doctors_process$doctor_id == '268068']
length(live_doctors_process$experience[is.na(live_doctors_process$experience)])

## 114,208 live doctors - INDIA
de <- live_doctors_process[,c(2,3,6)]
de <- de %>% filter(!is.na(experience))
## 79,322 live doctors - INDIA
quantile(de$experience, probs = .9995, na.rm = T)
de <- de %>% filter(experience <= 61)
de$conversion <- (de$transactions/de$users)*100
quantile(de$conversion, probs = .995, na.rm = T)
de <- de %>% filter(conversion <= 100)
## 78,916 live doctors - INDIA
summary(de)

de <- de %>% 
  mutate(buckets = ifelse(experience == 0, "no_exp", ifelse(experience < 8, "less_exp", ifelse(experience < 14, "average_exp", "more_exp")))) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

de_buckets <- de %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = de[,3:4], aes(x=experience)) + geom_density()

ggplot(de, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = de_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = de[,3:4], aes(x=experience, y=conversion)) + geom_point(size = 0.5, color = "red")
##### 9. DOCTOR ATTRIBUTE - PHOTOS ----
photos <- dbGetQuery(conn2,
"select 
pdp.doctor_id as doctor_id, 
pdp.id as practice_doctor_id,
count(dph.id) as Num_of_photos
from fabric_pii.doctor_photos_published dph
join fabric_pii.practice_doctors_published pdp on pdp.doctor_id=dph.doctor_id
join fabric_pii.practices_published pp on pdp.practice_id=pp.id
where dph.deleted_at is NULL 
and dph.photo_url is not NULL 
and pdp.profile_published=1
and photo_default = 'd8008fa8856c31f759b7518fb390e3e0432d5116'
group by 1,2
order by 1")

photos_process <- unique(photos[,c(1,3)])
photos_process$num_of_photos[photos_process$doctor_id == '268068']
live_doctors_process <- merge(live_doctors_process, photos_process, by = 'doctor_id', all.x = T)
live_doctors_process$num_of_photos[live_doctors_process$doctor_id == '268068']
length(live_doctors_process$num_of_photos[is.na(live_doctors_process$num_of_photos)])

## 114,208 live doctors - INDIA
dp <- live_doctors_process[,c(2,3,7)]
dp$num_of_photos[is.na(dp$num_of_photos)] <- 0
## 114,208 live doctors - INDIA
quantile(dp$num_of_photos, probs = .9999, na.rm = T)
# dp <- dp %>% filter(num_of_photos <= 4)
dp$conversion <- (dp$transactions/dp$users)*100
quantile(dp$conversion, probs = .99, na.rm = T)
dp <- dp %>% filter(conversion <= 100)
## 113,243 live doctors - INDIA
summary(dp)

dp <- dp %>% 
  mutate(buckets = ifelse(num_of_photos == 0, "no_photos", "photos")) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

dp_buckets <- dp %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = dp[,3:4], aes(x=num_of_photos)) + geom_density()

ggplot(dp, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = dp_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = dp[,3:4], aes(x=num_of_photos, y=conversion)) + geom_point(size = 0.5, color = "red")
##### 10. DOCTOR ATTRIBUTE - SERVICES ----
services <- dbGetQuery(conn2, "select pdp.doctor_id as doctor_id, 
pdp.id as practice_doctor_id,
count(dsp.service_id) as services
from fabric_pii.doctor_services_published dsp
join fabric_pii.master_doctor_services mds on mds.id = dsp.service_id
join fabric_pii.practice_doctors_published pdp on pdp.doctor_id=dsp.doctor_id
where pdp.deleted_at is NULL and pdp.profile_published=1 
and dsp.deleted_at is NULL
and mds.published=1
group by 1,2
order by 3 desc")

services_process <- unique(services[,c(1,3)])
services_process$services[services_process$doctor_id == '268068']
live_doctors_process <- merge(live_doctors_process, services_process, by = 'doctor_id', all.x = T)
live_doctors_process$services[live_doctors_process$doctor_id == '268068']
length(live_doctors_process$services[is.na(live_doctors_process$services)])

## 114,208 live doctors - INDIA
ds <- live_doctors_process[,c(2,3,8)]
# ds <- ds %>% filter(!is.na(services))
ds$services[is.na(ds$services)] <- 0
## 114,208 live doctors - INDIA
quantile(ds$services, probs = .99, na.rm = T)
# ds <- ds %>% filter(services <= 49)
ds$conversion <- (ds$transactions/ds$users)*100
quantile(ds$conversion, probs = .99, na.rm = T)
ds <- ds %>% filter(conversion <= 100)
## 112,119 live doctors - INDIA
summary(ds)

ds <- ds %>% 
  mutate(buckets = ifelse(services == 0, "no_services", ifelse(services <= 2, "less_services", ifelse(services <= 8, "average_services", "more_services")))) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

ds_buckets <- ds %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = ds[,3:4], aes(x=services)) + geom_density()

ggplot(ds, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = ds_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = ds[,3:4], aes(x=services, y=conversion)) + geom_point(size = 0.5, color = "red")
##### 11. DOCTOR ATTRIBUTE - MEMBERSHIPS ----
memberships <- dbGetQuery(conn2, "select pdp.doctor_id as doctor_id, 
pdp.id as practice_doctor_id, 
count(dmp.id) as memberships
from fabric_pii.doctor_memberships_published dmp
join fabric_pii.practice_doctors_published pdp on pdp.doctor_id=dmp.doctor_id
where pdp.deleted_at is NULL and pdp.profile_published=1 
and dmp.deleted_at is NULL and dmp.published = 1
group by 1,2
order by 1")

memberships_process <- unique(memberships[,c(1,3)])
memberships_process$memberships[memberships_process$doctor_id == '268068']
live_doctors_process <- merge(live_doctors_process, memberships_process, by = 'doctor_id', all.x = T)
live_doctors_process$memberships[live_doctors_process$doctor_id == '268068']
length(live_doctors_process$memberships[is.na(live_doctors_process$memberships)])

## 114,208 live doctors - INDIA
dm <- live_doctors_process[,c(2,3,9)]
# ds <- ds %>% filter(!is.na(services))
dm$memberships[is.na(dm$memberships)] <- 0
## 114,208 live doctors - INDIA
quantile(dm$memberships, probs = .99, na.rm = T)
# dm <- dm %>% filter(memberships <= 6)
dm$conversion <- (dm$transactions/dm$users)*100
quantile(dm$conversion, probs = .99, na.rm = T)
dm <- dm %>% filter(conversion <= 100)
## 112,394 live doctors - INDIA
summary(dm)

dm <- dm %>% 
  mutate(buckets = ifelse(memberships == 0, "no_memberships", "memberships")) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

dm_buckets <- dm %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = dm[,3:4], aes(x=memberships)) + geom_density()

ggplot(dm, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = dm_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = dm[,3:4], aes(x=memberships, y=conversion)) + geom_point(size = 0.5, color = "red")
##### 12. DOCTOR ATTRIBUTE - AWARDS ----
awards <- dbGetQuery(conn2, "select pdp.doctor_id as doctor_id, 
pdp.id as practice_doctor_id, 
count(dap.id) as awards
from fabric_pii.doctor_awards_published dap
join fabric_pii.practice_doctors_published pdp on pdp.doctor_id=dap.doctor_id
where pdp.deleted_at is NULL and pdp.profile_published=1 
and dap.deleted_at is NULL and dap.published=1
group by 1,2
order by 1")

awards_process <- unique(awards[,c(1,3)])
awards_process$awards[awards_process$doctor_id == '268068']
live_doctors_process <- merge(live_doctors_process, awards_process, by = 'doctor_id', all.x = T)
live_doctors_process$awards[live_doctors_process$doctor_id == '268068']
length(live_doctors_process$awards[is.na(live_doctors_process$awards)])

## 114,208 live doctors - INDIA
da <- live_doctors_process[,c(2,3,10)]
# ds <- ds %>% filter(!is.na(services))
da$awards[is.na(da$awards)] <- 0
## 114,208 live doctors - INDIA
quantile(da$awards, probs = .9999, na.rm = T)
# da <- da %>% filter(awards <= 6)
da$conversion <- (da$transactions/da$users)*100
quantile(da$conversion, probs = .99, na.rm = T)
da <- da %>% filter(conversion <= 100)
## 112,121 live doctors - INDIA
summary(da)

da <- da %>% 
  mutate(buckets = ifelse(awards == 0, "no_awards", "awards")) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

da_buckets <- da %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = da[,3:4], aes(x=awards)) + geom_density()

ggplot(da, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = da_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = da[,3:4], aes(x=awards, y=conversion)) + geom_point(size = 0.5, color = "red")
##### 13. DOCTOR ATTRIBUTE - CONSULTATION_FEES ----
fees <- dbGetQuery(conn2, "select pdp.doctor_id as doctor_id, 
pdp.id as practice_doctor_id, 
pdp.consultation_fee as fees
from fabric_pii.practice_doctors_published pdp
join fabric_pii.practices_published as pp on pdp.practice_id=pp.id
where pdp.deleted_at is NULL and pdp.profile_published=1 
--and pp.ray_practice_id is not null 
and pdp.consultation_fee is not null
order by 1")

fees$fees[fees$doctor_id == '268068']
fees_process <- (fees[,c(1,3)])
fees_process$fees[fees_process$doctor_id == '268068']
fees_process$fees <- as.integer(fees_process$fees)
quantile(as.integer(fees_process$fees), probs = .96, na.rm = T)
quantile(as.integer(fees_process$fees), probs = .1, na.rm = T)
fees_process <- fees_process %>% 
  filter(fees >= 50,
         fees <= 1500) %>%
  group_by(doctor_id) %>%
  summarise(fees = mean(fees)) %>%
  ungroup()
length(unique(fees_process$doctor_id))

live_doctors_process <- merge(live_doctors_process, fees_process, by = 'doctor_id', all.x = T)
live_doctors_process$fees[live_doctors_process$doctor_id == '268068']
length(live_doctors_process$fees[is.na(live_doctors_process$fees)])

## 114,208 live doctors - INDIA
df <- live_doctors_process[,c(2,3,11)]
# ds <- ds %>% filter(!is.na(services))
df <- df %>% filter(!is.na(fees))
## 91,507 live doctors - INDIA
quantile(df$fees, probs = .9999, na.rm = T)
# df <- df %>% filter(fees <= 1500)
df$conversion <- (df$transactions/df$users)*100
quantile(df$conversion, probs = .99, na.rm = T)
df <- df %>% filter(conversion <= 100)
## 91,507 live doctors - INDIA
summary(df)

df <- df %>% 
  mutate(buckets = ifelse(fees == 0, "no_fees", 
                          ifelse(fees <=150, "bin_150", 
                                 ifelse(fees <= 250, "bin_250", 
                                        ifelse(fees <= 350, "bin_350",
                                               ifelse(fees <= 500, "bin_500", 
                                                      ifelse(fees <= 1000, "bin_1000", "bin_1500"))))))) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

df_buckets <- df %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = df[,3:4], aes(x=fees)) + geom_density()
ggplot(data = df[,3:4], aes(x=fees)) + geom_histogram(bins = 100)

ggplot(data = df, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = df_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = df[,3:4], aes(x=fees, y=conversion)) + geom_point(size = 0.5, color = "red")

##### 13a. Tier-1 SPLIT of Consultation_fees attribute ----
doctor_city_map <- dbGetQuery(conn2, 
                              "select 
                              pdp.doctor_id as doctor_id,
                              c.name as city
                              from fabric_pii.practice_doctors_published pdp
                              join fabric_pii.practices_published pp on pp.id = pdp.practice_id
                              LEFT JOIN fabric_pii.master_localities l ON pp.locality_id = l.id
                              LEFT JOIN fabric_pii.master_cities c ON l.city_id = c.id
                              LEFT JOIN fabric_pii.master_states ms ON c.state_id = ms.id
                              LEFT JOIN fabric_pii.master_countries mcc ON ms.country_id = mcc.id
                              where mcc.name = 'India'
                              and pdp.deleted_at is NULL
                              and pdp.profile_published = 1")

live_doctors_process$city <- doctor_city_map$city[match(live_doctors_process$doctor_id, doctor_city_map$doctor_id)]
live_doctors_process_tier1 <- live_doctors_process %>% 
  filter(city %in% c('Bangalore', 'Hyderabad', 'Mumbai', 'Thane', 'Delhi', 'Gurgaon', 'Chennai', 'Pune', 'Navi Mumbai', 'Noida', 'Kolkata', 'Faridabad', 'Ghaziabad', 'Noida'))

## 83,926 live doctors - INDIA
df_tier1 <- live_doctors_process_tier1[,c(2,3,11)]
# ds <- ds %>% filter(!is.na(services))
df_tier1 <- df_tier1 %>% filter(!is.na(fees))
## 65,780 live doctors - INDIA
quantile(df_tier1$fees, probs = .9999, na.rm = T)
# df <- df %>% filter(fees <= 1500)
df_tier1$conversion <- (df_tier1$transactions/df_tier1$users)*100
quantile(df_tier1$conversion, probs = .99, na.rm = T)
df_tier1 <- df_tier1 %>% filter(conversion <= 100)
## 91,507 live doctors - INDIA
summary(df_tier1)

df_tier1 <- df_tier1 %>% 
  mutate(buckets = ifelse(fees == 0, "no_fees", 
                          ifelse(fees <=150, "bin_150", 
                                 ifelse(fees <= 250, "bin_250", 
                                        ifelse(fees <= 350, "bin_350",
                                               ifelse(fees <= 500, "bin_500", 
                                                      ifelse(fees <= 1000, "bin_1000", "bin_1500"))))))) %>%
  group_by(buckets) %>%
  mutate(bucket_avg = (sum(transactions)/sum(users))*100) %>%
  ungroup()

df_tier1_buckets <- df_tier1 %>%
  group_by(buckets) %>%
  count(buckets) %>%
  ungroup() %>%
  mutate(perc = (n/sum(n)))

ggplot(data = df_tier1[,3:4], aes(x=fees)) + geom_density()
ggplot(data = df_tier1[,3:4], aes(x=fees)) + geom_histogram(bins = 100)

ggplot(data = df_tier1, aes(x=buckets)) + 
  geom_point(aes(x=buckets, y=bucket_avg), size = 1.5, color = "deepskyblue4") + 
  geom_bar(data = df_tier1_buckets, stat = "identity", aes(x=buckets, y=perc), fill = "deepskyblue2")

ggplot(data = df_tier1[,3:4], aes(x=fees, y=conversion)) + geom_point(size = 0.5, color = "red")  
##### 14. DOCTOR ATTRIBUTE - RECOMMENDATIONS ----
recos <- read.csv("./Doc recos.csv", stringsAsFactors = F)

map <- dbGetQuery(conn2,
"select 
pdp.id as practice_doctor_id,
pdp.doctor_id as doctor_id
from fabric_pii.practice_doctors_published pdp
join fabric_pii.practices_published pp on pp.id = pdp.practice_id
where pdp.deleted_at is NULL
and pdp.profile_published = 1")

recos$doctor_id <- map$doctor_id[match(recos$doctor_practice_id, map$practice_doctor_id)]
recos_process_before <- recos %>%
  filter(date < '2017-10-01') %>%
  group_by(doctor_id) %>%
  summarise(recos_before = sum(recos)) %>%
  ungroup() 

recos_process_after <- recos %>%
  filter(date >= '2017-10-01', date < '2017-12-01') %>%
  group_by(doctor_id) %>%
  summarise(recos_after = sum(recos)) %>%
  ungroup()

recos_process <- merge(recos_process_before, recos_process_after, by = 'doctor_id', all.x = T, all.y = T)
recos_process$recos_after[is.na(recos_process$recos_after)] <- 0





