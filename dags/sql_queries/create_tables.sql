CREATE TABLE IF NOT EXISTS public.artists (
	"artistid" varchar(256) NOT NULL,
	"name" varchar(256),
	"location" varchar(256),
	"lattitude" numeric(18,0),
	"longitude" numeric(18,0)
);

CREATE TABLE IF NOT EXISTS public.songplays (
	"playid" varchar(32) NOT NULL,
	"start_time" timestamp NOT NULL,
	"userid" int NOT NULL,
	"level" varchar(256),
	"songid" varchar(256),
	"artistid" varchar(256),
	"sessionid" int,
	"location" varchar(256),
	"user_agent" varchar(256),
	CONSTRAINT "songplays_pkey" PRIMARY KEY ("playid")
);

CREATE TABLE IF NOT EXISTS public.songs (
	"songid" varchar(256) NOT NULL,
	"title" varchar(256),
	"artistid" varchar(256),
	"year" int,
	"duration" numeric(18,0),
	CONSTRAINT "songs_pkey" PRIMARY KEY ("songid")
);

CREATE TABLE  IF NOT EXISTS public.staging_events (
	"artist" varchar(256),
	"auth" varchar(256),
	"firstname" varchar(256),
	"gender" varchar(256),
	"iteminsession" DECIMAL(10,0),
	"lastname" varchar(256),
	"length" numeric(18,0),
	"level" varchar(256),
	"location" varchar(256),
	"method" varchar(256),
	"page" varchar(256),
	"registration" numeric(18,0),
	"sessionid" int,
	"song" varchar(256),
	"status" int,
	"ts" int,
	"useragent" varchar(256),
	"userid" int
);

CREATE TABLE  IF NOT EXISTS public.staging_songs (
	"num_songs" int,
	"artist_id" varchar(256),
	"artist_name" varchar(256),
	"artist_latitude" numeric(18,0),
	"artist_longitude" numeric(18,0),
	"artist_location" varchar(256),
	"song_id" varchar(256),
	"title" varchar(256),
	"duration" numeric(18,0),
	"year" int
);

CREATE TABLE  IF NOT EXISTS public.time (
	"start_time" timestamp NOT NULL,
	"hour" int,
	"day" int,
	"week" int,
	"month" varchar(256),
	"year" int,
	"weekday" varchar(256),
	CONSTRAINT "time_pkey" PRIMARY KEY ("start_time")
) ;

CREATE TABLE  IF NOT EXISTS public.users (
	"userid" int NOT NULL,
	"first_name" varchar(256),
	"last_name" varchar(256),
	"gender" varchar(256),
	"level" varchar(256),
	CONSTRAINT "users_pkey" PRIMARY KEY ("userid")
);
