set define off

CREATE OR REPLACE EDITIONABLE PACKAGE "SPOTIFY"."SPOTIFY_API" is
    --
    g_expiry timestamp;
    --
    FUNCTION get_token return varchar2;
    procedure load_playlists;
    procedure load_pl_tracks;
    procedure load_user_tracks;
    procedure load_track_detail;
    procedure tracks_to_playlist (
      p_dest_pl   in varchar2,
      p_src_pl    in varchar2 default null,
      p_t_id      in varchar2 default null, 
      p_keep_yn   in varchar2 default 'N');

END spotify_api;
/
