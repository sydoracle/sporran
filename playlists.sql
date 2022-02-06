CREATE TABLE SPOTIFY.PLAYLISTS
    (pl_id          varchar2(100),
    pl_name         varchar2(400),
    pl_owner_name   varchar2(400),
    pl_desc         varchar2(2000),
    pl_tracks_url   varchar2(400),
    pl_tracks_cnt   number,
    pl_json         VARCHAR2(4000)
    );
