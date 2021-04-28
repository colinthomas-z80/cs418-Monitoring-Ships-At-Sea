# nprenet@bsu.edu
# CS418, Spring 2021


drop database if exists AISDraft;

create database AISDraft;

use AISDraft;

## Create here the VESSEL table:
create table VESSEL(
	IMO mediumint unsigned,
	Flag varchar(40),
	Name varchar(128),
	Built smallint,
	Callsign varchar(8),
	Length smallint,
	Breadth tinyint,
	Tonnage mediumint,
	MMSI int unsigned,
	Type varchar(30),
	Status varchar(40),
	Owner varchar(80),
	primary key(IMO)
);



###########################


create table MAP_VIEW(
	Id mediumint,
	Name varchar(10),
	LongitudeW decimal(9,6),
	LatitudeS decimal(8,6),
	LongitudeE decimal(9,6),
	LatitudeN decimal(8,6),
	Scale enum('1', '2', '3'),
	RasterFile varchar(100),
	ImageWidth smallint,
	ImageHeight smallint,
	ActualLongitudeW decimal(9,6),
	ActualLatitudeS decimal(8,6),
	ActualLongitudeE decimal(9,6),
	ActualLatitudeN decimal(8,6),
	ContainerMapView_Id mediumint,
	foreign key (ContainerMapView_Id) references MAP_VIEW(Id),
	primary key( Id )
);



create table AIS_MESSAGE(
	Id mediumint unsigned,
	Timestamp datetime,
	MMSI int,
	Class enum('Class A','Class B','AtoN','Base Station'),
	Vessel_IMO mediumint unsigned,
	foreign key (Vessel_IMO) references VESSEL(IMO),
	primary key (Id)
);


## Create here the PORT table:

create table PORT(
	Id mediumint unsigned,
	LoCode varchar(5),
	Name varchar(30),
	Country varchar(15),
	Longitude decimal(9,6),
	Latitude decimal(9,6),
	Website varchar(500),
	MapView1_Id mediumint,
	MapView2_Id mediumint,
	MapView3_Id mediumint,
	primary key (Id),
	foreign key(MapView1_Id) references MAP_VIEW(Id),
	foreign key(MapView2_Id) references MAP_VIEW(Id),
	foreign key(MapView3_Id) references MAP_VIEW(Id)
);


###########################



## Create here the STATIC_DATA table:

create table STATIC_DATA(
	AISMessage_Id mediumint unsigned,
	AISIMO int,
	CallSign varchar(8),
	Name varchar(30),
	VesselType varchar(30),
	CargoType varchar(30),
	Length smallint,
	Breadth tinyint,
	Draught tinyint,
	AISDestination varchar(50),
	ETA datetime,
	A int,
	B int,
	C int,
	D int,
	primary key (AISMessage_Id)
);


###########################



create table POSITION_REPORT(
	AISMessage_Id mediumint unsigned,
	NavigationalStatus varchar(40),
	Longitude decimal(9,6),
	Latitude decimal(8,6),
	RoT decimal(4,1),
	SoG decimal(4,1),
	CoG decimal(4,1),
	Heading smallint,
	LastStaticData_Id mediumint unsigned,
	MapView1_Id mediumint,
	MapView2_Id mediumint,
	MapView3_Id mediumint,
	foreign key (LastStaticData_Id) references STATIC_DATA(AISMessage_Id),
	foreign key (MapView1_Id) references MAP_VIEW(Id),
	foreign key (MapView2_Id) references MAP_VIEW(Id),
	foreign key (MapView3_Id) references MAP_VIEW(Id),
	primary key (AISMessage_Id)
);


create table USER(
	EmailAddress varchar(30),
	Name varchar(20),
	PasswordHash char(36),
	ApplicationSetting1 bool,
	ApplicationSetting2 varchar(50),
	ApplicationSetting3 tinyint,
	primary key (EmailAddress)
);




## Create here the MONITOR_VESSEL table:

create table MONITOR_VESSEL(
	User_EmailAddress varchar(30),
	Vessel_IMO mediumint unsigned,
	DateStarted datetime,
	DateEnded datetime,
	foreign key(User_EmailAddress) references USER(EmailAddress),
	foreign key(Vessel_IMO) references VESSEL(IMO)
);

###########################



## Create here the MONITOR_PORT table:

create table MONITOR_PORT(
	User_EmailAddress varchar(30),
	Port_Id mediumint unsigned,
	DateStarted datetime,
	DateEnded datetime,
	foreign key(User_EmailAddress) references USER(EmailAddress),
	foreign key(Port_Id) references PORT(Id)
);


