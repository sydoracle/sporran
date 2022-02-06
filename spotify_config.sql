create table spotify_config
    (config_name varchar2(200) not null,
    config_value varchar2(4000)
);

alter table spotify_config add constraint pky_spotify_config primary key (config_name);
