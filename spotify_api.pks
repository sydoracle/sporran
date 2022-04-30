set define off

create or replace PACKAGE spotify_api is
    FUNCTION get_token return varchar2;
    procedure load_playlists;
    procedure load_pl_tracks;
    procedure load_user_tracks;
END spotify_api;
/
