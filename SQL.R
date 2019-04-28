# connect
install.packages('RPostgreSQL')
require('RPostgreSQL')
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname = 'loan',
                 host = 's19db.apan5310.com', port = 50102,
                 user = 'postgres', password = 'a3acvk9r')

###### Extract
# create tables
stmt <- "
create table purchaser(
purchaser_type_id serial primary key,
purchaser_type_name varchar(200));

create table agency(
agency_id serial primary key, 
agency_name varchar(100), 
agency_abbr varchar(20));

create table property_type(
property_type_id serial primary key, 
property_type_name varchar(100));

create table owner_occupancy(
owner_occupancy_id serial primary key, 
owner_occupancy_name varchar(100));

create table census_tract(
census_tract_id	serial primary key,
census_tract_number varchar(20), 
county_name varchar(30), 
state_name varchar(50), 
state_abbr varchar(20),
msamd_name varchar(100), 
tract_to_msamd_income numeric(12,8), 
population integer, 
minority_population numeric(12,8), 
number_of_owner_occupied_units integer, 
number_of_1_to_4_family_units integer, 
hud_median_family_income integer);	   

create table loan_type(
loan_type_id serial primary key, 
loan_type_name varchar(100));

create table loan_purpose( 
Loan_purpose_id serial primary key, 
loan_purpose_name varchar(100));

create table lien_status (
lien_status_id serial primary key, 
lien_status_name	varchar(30) 
);

create table action_taken (
action_taken_id serial primary key, 
action_taken_name	varchar(100)	
);

create table applicant (
applicant_id	serial primary key,
sex_name	varchar(100),
race_name	varchar(100),
race_mixed	varchar(100),
ethinicity_name	varchar(100)
);

create table denial_reason(
denial_reason_id 	serial primary key,
denial_reason_name	varchar(100));

CREATE TABLE preapproval(
preapproval_id 		serial PRIMARY KEY,
preapproval_name	varchar(100));

CREATE TABLE respondent(
respondent_id 		serial PRIMARY KEY,
respondent_real_id	varchar(50));

CREATE TABLE loan(
loan_id		integer,
loan_amount_000s	numeric(9,2),
respondent_id varchar(50),
preapproval_id	integer,
loan_type_id	integer,
loan_purpose_id	integer,
lien_status_id	integer,
action_taken_id	integer, 
applicant_income_000s integer, 
census_tract_id integer,
property_type_id	integer,
purchaser_id	integer,
owner_occupancy_id	integer,
agency_id	integer,
applicant_id integer,
co_applicant_id integer,
primary key (loan_id),
foreign key (preapproval_id) references preapproval(preapproval_id),
foreign key (loan_type_id) references loan_type(loan_type_id),
foreign key (loan_purpose_id) references loan_purpose(loan_purpose_id),
foreign key (lien_status_id) references lien_status(lien_status_id),
foreign key (action_taken_id) references action_taken(action_taken_id),
foreign key (census_tract_id) references census_tract(census_tract_id),
foreign key (property_type_id) references property_type(property_type_id),
foreign key (purchaser_id) references purchaser(purchaser_type_id),
foreign key (owner_occupancy_id) references owner_occupancy(owner_occupancy_id),
foreign key (agency_id) references agency(agency_id),
foreign key (applicant_id) references applicant(applicant_id),
foreign key (co_applicant_id) references applicant(applicant_id)
);

CREATE TABLE loan_denial_reason(
loan_id		serial,
reason_id		serial,
PRIMARY KEY (loan_id, reason_id),
FOREIGN KEY (loan_id) references loan (loan_id),
FOREIGN KEY (reason_id) references denial_reason (denial_reason_id));
"

dbGetQuery(con,stmt)



# load csv
df <- read.csv(file="team 2_dataset.csv")

stmt <- "
create table raw_data (
tract_to_msamd_income     numeric(12,8),
rate_spread     numeric(5,2),
population     integer,
minority_population     numeric(12,8),
number_of_owner_occupied_units     integer,
number_of_1_to_4_family_units     integer,
loan_amount_000s     numeric(9,2),
hud_median_family_income     integer,
applicant_income_000s     numeric(9,2),
state_name     varchar(50),
state_abbr     varchar(20),
sequence_number     varchar(50),
respondent_id     varchar(50),
purchaser_type_name     varchar(200),
property_type_name     varchar(100),
preapproval_name     varchar(100),
owner_occupancy_name     varchar(100),
msamd_name     varchar(100),
loan_type_name     varchar(100),
loan_purpose_name     varchar(100),
lien_status_name     varchar(30),
hoepa_status_name     varchar(50),
edit_status_name     varchar(50),
denial_reason_name_3     varchar(100),
denial_reason_name_2     varchar(100),
denial_reason_name_1     varchar(100),
county_name     varchar(100),
co_applicant_sex_name     varchar(100),
co_applicant_race_name_5     varchar(100),
co_applicant_race_name_4     varchar(100),
co_applicant_race_name_3     varchar(100),
co_applicant_race_name_2     varchar(100),
co_applicant_race_name_1     varchar(100),
co_applicant_ethnicity_name     varchar(100),
census_tract_number     varchar(20),
as_of_year     varchar(50),
application_date_indicator     varchar(50),
applicant_sex_name     varchar(100),
applicant_race_name_5     varchar(100),
applicant_race_name_4     varchar(100),
applicant_race_name_3     varchar(100),
applicant_race_name_2     varchar(100),
applicant_race_name_1     varchar(100),
applicant_ethnicity_name     varchar(100),
agency_name     varchar(100),
agency_abbr     varchar(20),
action_taken_name     varchar(100)
)
"

dbGetQuery(con,stmt)
dbWriteTable(con, name="raw_data", value=df, row.names=FALSE, append=TRUE)

###### Transform
# transform data 1: Add 'loan_id' into the 'raw_data' table
stmt <- "
ALTER TABLE raw_data
add loan_id serial;
"
dbGetQuery(con,stmt)


# transform data 2: Create raw_data_data table with adding 'race_mixed' and drop lines having NULL in census_tract
stmt <- "
CREATE TABLE raw_data_data as
select *,
case when applicant_race_name_2 = '' then 'no'
else 'yes'
end as race_mixed,
case when co_applicant_race_name_2 = '' then 'no'
else 'yes'
end as co_race_mixed
from raw_data
where census_tract_number is NOT NULL;
"
dbGetQuery(con,stmt)


# transform data 3: Make VIEW to combine applicant and co_applicant
stmt <- "
CREATE VIEW applicant_temp as
select applicant_sex_name as sex_name,
       applicant_race_name_1 as race_name,
       race_mixed as race_mixed,
       applicant_ethnicity_name as ethinicity_name
from raw_data_data 
union all
select co_applicant_sex_name as sex_name,
       co_applicant_race_name_1 as race_name,
       co_race_mixed as race_mixed,
       co_applicant_ethnicity_name as ethinicity_name
from raw_data_data;
"
dbGetQuery(con,stmt)


###### Load
# insert data
stmt <- "
insert into respondent(respondent_real_id)
select respondent_id
from raw_data_data
group by respondent_id;

insert into owner_occupancy(owner_occupancy_name)
select owner_occupancy_name
from raw_data_data
group by owner_occupancy_name;

insert into census_tract(census_tract_number,county_name, state_name, msamd_name,tract_to_msamd_income, population, minority_population, number_of_owner_occupied_units, number_of_1_to_4_family_units, hud_median_family_income)
select census_tract_number,county_name, state_name, msamd_name,tract_to_msamd_income, population, minority_population, number_of_owner_occupied_units, number_of_1_to_4_family_units, hud_median_family_income
from raw_data_data
group by census_tract_number,county_name, state_name, msamd_name,tract_to_msamd_income, population, minority_population, number_of_owner_occupied_units, number_of_1_to_4_family_units, hud_median_family_income;

insert into loan_type(loan_type_name)
select loan_type_name
from raw_data_data
group by loan_type_name;

insert into loan_purpose(loan_purpose_name)
select loan_purpose_name
from raw_data_data
group by loan_purpose_name;

insert into lien_status (lien_status_name)
select lien_status_name
from raw_data_data
group by lien_status_name;

insert into action_taken (action_taken_name)
select action_taken_name
from raw_data_data
group by action_taken_name;

insert into applicant (sex_name, race_name, race_mixed, ethinicity_name)
select sex_name, race_name, race_mixed, ethinicity_name
from applicant_temp
group by sex_name, race_name, race_mixed, ethinicity_name;

insert into denial_reason (denial_reason_name)
select x.denial_reason_name
from (
select denial_reason_name_1 as denial_reason_name
from raw_data_data
union all
select denial_reason_name_2 as denial_reason_name
from raw_data_data
union all
select denial_reason_name_3 as denial_reason_name
from raw_data_data
) as x
where x.denial_reason_name != ''
group by x.denial_reason_name;

insert into preapproval (preapproval_name)
select preapproval_name
from raw_data_data
group by preapproval_name;

insert into purchaser(purchaser_type_name)
select purchaser_type_name
from raw_data_data
group by purchaser_type_name;

insert into agency(agency_name, agency_abbr)
select agency_name, agency_abbr
from raw_data_data
group by agency_name, agency_abbr;

insert into property_type(property_type_name)
select property_type_name
from raw_data_data
group by property_type_name;

insert into loan(loan_id, respondent_id, loan_amount_000s, preapproval_id, loan_type_id,
loan_purpose_id, lien_status_id, action_taken_id, applicant_income_000s,
property_type_id, purchaser_id, owner_occupancy_id,
agency_id, applicant_id, co_applicant_id, census_tract_id)
select r.loan_id, rp.respondent_id, r.loan_amount_000s, p.preapproval_id, lt.loan_type_id,
lp.loan_purpose_id, ls.lien_status_id, at.action_taken_id, r.applicant_income_000s,
pt.property_type_id, pu.purchaser_type_id, oo.owner_occupancy_id, a.agency_id, ap.applicant_id, cp.applicant_id, ct.census_tract_id
from raw_data_data r, preapproval p, loan_type lt, loan_purpose lp, lien_status ls, action_taken at, respondent rp,
property_type pt, purchaser pu, owner_occupancy oo,  agency a, applicant ap, applicant cp, census_tract ct
where r.preapproval_name = p.preapproval_name AND
r.respondent_id = rp.respondent_real_id AND
r.loan_type_name = lt.loan_type_name AND
r.loan_purpose_name = lp.loan_purpose_name AND
r.lien_status_name = ls.lien_status_name AND
r.action_taken_name = at.action_taken_name AND
r.property_type_name = pt.property_type_name AND
r.purchaser_type_name = pu.purchaser_type_name AND
r.owner_occupancy_name = oo.owner_occupancy_name AND
r.agency_name = a.agency_name AND
(r.applicant_race_name_1 =  ap.race_name AND r.applicant_sex_name =  ap.sex_name AND 
r.applicant_ethnicity_name =  ap.ethinicity_name AND r.race_mixed =  ap.race_mixed) AND
(r.co_applicant_race_name_1 =  cp.race_name AND r.co_applicant_sex_name =  cp.sex_name AND 
r.co_applicant_ethnicity_name =  cp.ethinicity_name AND r.co_race_mixed =  cp.race_mixed) AND
(r.county_name = ct.county_name AND r.state_name = ct.state_name AND 
r.msamd_name = ct.msamd_name  AND r.tract_to_msamd_income = ct.tract_to_msamd_income  AND r.population = ct.population AND
r.minority_population = ct.minority_population AND r.number_of_owner_occupied_units = ct.number_of_owner_occupied_units AND
r.number_of_1_to_4_family_units = ct.number_of_1_to_4_family_units AND r.hud_median_family_income = ct.hud_median_family_income)
;


insert into loan_denial_reason(loan_id, reason_id)
select r.loan_id, d.denial_reason_id
from denial_reason d, raw_data_data r
where r.denial_reason_name_1 =  d.denial_reason_name
AND r.denial_reason_name_1 != '';

insert into loan_denial_reason(loan_id, reason_id)
select r.loan_id, d.denial_reason_id
from denial_reason d, raw_data_data r
where r.denial_reason_name_2 =  d.denial_reason_name
AND r.denial_reason_name_2 != '';

insert into loan_denial_reason(loan_id, reason_id)
select r.loan_id, d.denial_reason_id
from denial_reason d, raw_data_data r
where r.denial_reason_name_3 =  d.denial_reason_name
AND r.denial_reason_name_3 != '';
"

dbGetQuery(con,stmt)
