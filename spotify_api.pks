set define off

create or replace PACKAGE spotify_api is
    --
    g_expiry timestamp;
    --
    FUNCTION get_token return varchar2;
    procedure load_playlists;
    procedure load_pl_tracks;
    procedure load_artists;
    procedure load_user_tracks;
    procedure load_track_detail;
	procedure track_search (p_term in varchar2);
    procedure del_dups;
    procedure get_follows;
    procedure add_follows;
    procedure order_tracks (p_name in varchar2 default '%') ;
    procedure balance_playlists (p_pl_name1 in varchar2, p_pl_name2 in varchar2);
    procedure tracks_to_playlist (
      p_dest_pl   in varchar2,
      p_src_pl    in varchar2 default null,
      p_t_id      in varchar2 default null, 
      p_keep_yn   in varchar2 default 'N',
      p_artist    in varchar2 default null);
    procedure set_pl_desc (p_pl_id in varchar2, p_desc in varchar2);
END spotify_api;
/
