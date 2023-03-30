create table chicago_crimes_details (
  ID int,
  case_number varchar(100),
  "date" varchar(100),
  Block varchar(100),
  IUCR varchar(100),
  primary_type varchar(100),
  Description  varchar(100),
  location_description  varchar(100),
  Arrest  bool,
  Domestic bool,
  Beat varchar(100),
  District int,
  Ward  int,
  community_area  int,
  FBI_code varchar(100),
  X_Coordinate double precision,
  Y_Coordinate double precision,
  "Year" int,
  Updated_On  varchar(100),
  Latitude varchar(100),
  Longitude varchar(100),
  Location varchar(100)
);

create table chicago_crimes_by_date_district (
  crime_date varchar(100),
  ID int,
  district int
);
  
create table chicago_crimes_by_ward_arrest (
  crime_date varchar(100),
  primary_type varchar(100),
  ward int,
  arrest int
);
  