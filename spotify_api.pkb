set define off

CREATE OR REPLACE PACKAGE spotify_api is
    FUNCTION get_token return varchar2;
    procedure load_playlists;
END spotify_api;
/

CREATE OR REPLACE PACKAGE BODY spotify_api is
    --
    FUNCTION get_token return varchar2 is
        v_client_id     varchar2(1000);
        v_client_secret varchar2(1000);
        v_refresh_token varchar2(1000);
        v_access_token  varchar2(1000);
    BEGIN
        select  max(case when config_name = 'CLIENT_ID' then config_value end) client_id,
                max(case when config_name = 'CLIENT_SECRET' then config_value end) client_secret,
                max(case when config_name = 'REFRESH_TOKEN' then config_value end) refresh_token
        into v_client_id, v_client_secret, v_refresh_token
        from spotify_config;
        --
        -- call the oauth_authenticate procedure to get a Token, 
        -- based on your Client_Id and Client_Secret and the existing refresh token
        select access_token
        into  v_access_token
        from json_table(fnc_url_to_clob (
                p_url       =>  'https://accounts.spotify.com/api/token',
                p_method    =>  'POST', 
                p_header    =>  'Content-Type: application/x-www-form-urlencoded', 
                p_post_data => 'client_id='||v_client_id||'&client_secret='||v_client_secret||'&grant_type=refresh_token&refresh_token='||v_refresh_token),
                    '$'
                        columns (
                            access_token        varchar2(400) PATH '$.access_token' null on empty error on error,
                            token_type          varchar2(400) PATH '$.token_type' null on empty error on error,
                            expires_in          number PATH '$.expires_in' null on empty error on error,
                            scope               varchar2(400) PATH '$.scope' null on empty error on error,
                            refresh_token       varchar2(400) PATH '$.refresh_token' null on empty error on error
                            )
                ) j;
        --
        return v_access_token;
        --
    END get_token; 
    --
    procedure load_playlists is
        v_offset    number := 1;
        v_token     varchar2(2000);
    begin
        v_token := spotify_api.get_token;
        delete from playlists;
        while v_offset is not null loop
            insert into playlists
                (pl_id, pl_name, pl_owner_name, pl_desc, pl_tracks_url, pl_tracks_cnt, pl_json)
            select j.*
            from json_table(fnc_ws_get_json(
                        'https://api.spotify.com/v1/me/playlists?limit=50&offset='||v_offset,
                        'Bearer '||v_token),  '$.items[*]'
                        columns  (
                            pl_id           varchar2(800) PATH '$.id',
                            pl_name         varchar2(800) PATH '$.name',
                            pl_owner_name   varchar2(800) PATH '$.owner.display_name',
                            pl_desc         varchar2(2000) PATH '$.description',
                            pl_tracks_url   varchar2(800) PATH '$.tracks.href',
                            pl_tracks_cnt   number PATH '$.tracks.total',
                            pl_json  varchar2(4000) FORMAT JSON PATH '$')
                        ) j;
            dbms_session.sleep(2);
            if sql%rowcount = 0 then
                v_offset := null;
            elsif v_offset > 1000 then
                v_offset := null;
            else
                v_offset := v_offset + 50;
            end if;
        end loop;
    end load_playlists;
    --
    --
    /*
    for r_main in c_pl loop
        dbms_application_info.set_client_info('Playlist track json'||r_main.pl_name);
        update spotify_playlist
        set tracks_1_json =  api_get_json(r_main.pl_tracks_url||'?limit=50&offset=0','Bearer '||v_token)
        where pl_id = r_main.pl_id;
        --
        if r_main.pl_tracks_cnt > 49 then
            update spotify_playlist
            set tracks_2_json =  api_get_json(r_main.pl_tracks_url||'?limit=50&offset=49','Bearer '||v_token)
            where pl_id = r_main.pl_id;
        end if;
        if r_main.pl_tracks_cnt > 98 then
            update spotify_playlist
            set tracks_3_json =  api_get_json(r_main.pl_tracks_url||'?limit=50&offset=98','Bearer '||v_token)
            where pl_id = r_main.pl_id;
        end if;
        dbms_session.sleep(2);
    end loop;
    commit;
    */
    --
end spotify_api;
/

BEGIN
    spotify_api.load_playlists;
end;
/