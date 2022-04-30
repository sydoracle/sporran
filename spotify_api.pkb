set define off

create or replace PACKAGE BODY spotify_api is
    --
    g_token varchar2(32000);
    g_expiry timestamp;
    --
    FUNCTION get_token return varchar2 is
        v_client_id     varchar2(1000);
        v_client_secret varchar2(1000);
        v_refresh_token varchar2(1000);
        v_access_token  varchar2(1000);
        v_response      varchar2(32000);
        v_expiry        number;
    BEGIN
        if g_expiry is not null
        and g_expiry < systimestamp then 
            return g_token;
        end if;
        --
        select  max(case when config_name = 'CLIENT_ID' then config_value end) client_id,
                max(case when config_name = 'CLIENT_SECRET' then config_value end) client_secret,
                max(case when config_name = 'REFRESH_TOKEN' then config_value end) refresh_token
        into v_client_id, v_client_secret, v_refresh_token
        from spotify_config;
        --
        -- call the oauth_authenticate procedure to get a Token, 
        -- based on your Client_Id and Client_Secret and the existing refresh token
        v_response := fnc_url_to_clob (
                p_url       =>  'https://accounts.spotify.com/api/token',
                p_method    =>  'POST', 
                p_header    =>  'Content-Type: application/x-www-form-urlencoded', 
                p_post_data => 'client_id='||v_client_id||'&client_secret='||v_client_secret||'&grant_type=refresh_token&refresh_token='||v_refresh_token);
        --
        if v_response is JSON then
            select access_token, expires_in
            into  v_access_token, v_expiry
            from json_table(v_response, '$'
                        columns (
                            access_token        varchar2(400) PATH '$.access_token' null on empty error on error,
                            token_type          varchar2(400) PATH '$.token_type' null on empty error on error,
                            expires_in          number PATH '$.expires_in' null on empty error on error,
                            scope               varchar2(400) PATH '$.scope' null on empty error on error,
                            refresh_token       varchar2(400) PATH '$.refresh_token' null on empty error on error
                            )
                ) j;
        else
            raise_application_error(-20001,'Error:'||v_response);
        end if;
        --
        g_token := v_access_token;
        g_expiry := systimestamp + (v_expiry * interval '1' second);
        return v_access_token;
        --
    END get_token; 
    --
    procedure load_playlists is
        v_offset    number := 0;
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
    procedure user_tracks_to_playlist 
    is
        cursor c_main is
            select json_value(t_artists,'$[0].name') artist, 
                    listagg('spotify:track:'||t_id,',') within group (order by t_id) tracks
            from user_tracks
            where t_id not in (select pl.t_id from PLAYLIST_TRACKS pl)
            group by json_value(t_artists,'$[0].name')
            order by 1;
        v_res varchar2(32000);
        v_pl_id varchar2(100);
    begin
        --
        select pl_id 
        into v_pl_id
        from playlists
        where pl_name = 'Todo';
        --
        for r_main in c_main loop
            v_res := fnc_url_to_clob (
                        p_url       =>  'https://api.spotify.com/v1/playlists/'||v_pl_id||'/tracks?uris='||r_main.tracks,
                        p_method    =>  'POST', 
                        p_header    =>  'Authorization: Bearer '||spotify_api.get_token,
                        p_post_data => '{"position": 0}'
                        );
            dbms_session.sleep(2);
        end loop;
    end user_tracks_to_playlist;
    --
    procedure load_user_tracks is
        v_offset    number := 0;
        v_token     varchar2(2000);
    begin
        v_token := spotify_api.get_token;
        v_offset := 0;
        while v_offset <= 10000 loop
            insert into user_tracks
                (added_seq, t_added, t_id, t_name, t_explicit, t_duration, 
                t_album_name, t_album_id, t_release_dt, t_artists, t_album, t_track_json)
            select added_seq, to_date(t_added,'YYYY-MM-DD"T"HH24:MI:SS"Z"') t_added, 
                    t_id, t_name, t_explicit, t_duration, 
                    t_album_name, t_album_id, t_release_dt, t_artists, t_album, t_track_json
            from json_table(fnc_ws_get_json('https://api.spotify.com/v1/me/tracks?limit=50&offset='||v_offset, 
                        'Bearer '||v_token), '$.items[*]'
                columns  (
                    added_seq   FOR ORDINALITY,
                    t_added		   varchar2(60)  PATH '$.added_at',
                    t_id           varchar2(800) PATH '$.track.id',
                    t_name         varchar2(800) PATH '$.track.name',
                    t_explicit     varchar2(800) PATH '$.track.explicit',
                    t_duration     number        PATH '$.track.duration_ms',
                    t_album_name   varchar2(800)  PATH '$.track.album.name',
                    t_album_id     varchar2(800)  PATH '$.track.album.id',
                    t_release_dt   varchar2(800)  PATH '$.track.album.release_date',
                    t_artists      varchar2(4000) FORMAT JSON PATH '$.track.artists',
                    t_album        varchar2(4000) FORMAT JSON PATH '$.track.album',
                    t_track_json   clob FORMAT JSON PATH '$' error on error
                    )
                ) j
            where t_name is not null
            and t_id not in (select u.t_id from user_tracks u);
            --
            v_offset := v_offset + 50;
            commit;
            dbms_session.sleep(2);
            --
        end loop;
        --
        user_tracks_to_playlist;
        commit;
        --
    end load_user_tracks;
    --
    procedure load_pl_tracks is
        v_offset    number := 0;
        v_token     varchar2(2000);
        cursor c_pl is
            select pl_id, pl_tracks_url, pl_tracks_cnt, pl_name, pl_owner_name
            from playlists
            order by pl_owner_name, pl_name, pl_id;
    begin
        v_token := spotify_api.get_token;
        for r_pl in c_pl loop
            dbms_application_info.set_client_info(r_pl.pl_owner_name||'.'||r_pl.pl_name);
            dbms_output.put_line(r_pl.pl_owner_name||'.'||r_pl.pl_name||' '||to_char(sysdate,'HH24:MI:SS'));
            delete from playlist_tracks
            where pl_id = r_pl.pl_id;
            --
            v_offset := 0;
            while v_offset <= r_pl.pl_tracks_cnt loop
                insert into playlist_tracks
                select  r_pl.pl_id pl_id, j.playlist_seq, j.t_id, j.t_name, 
                        j.t_explicit, round(j.t_duration/(1000*60),1) t_dur_mins, 
                        j.t_album_name, j.t_album_id, j.t_release_dt, 
                        j.t_artists, j.t_album, j.t_track_json
                from json_table(fnc_ws_get_json(r_pl.pl_tracks_url||'?limit=50&offset='||v_offset, 'Bearer '||v_token),
                        '$.items[*].track'
                        columns  (
                            playlist_seq   FOR ORDINALITY,
                            t_id           varchar2(800) PATH '$.id',
                            t_name         varchar2(800) PATH '$.name',
                            t_explicit     varchar2(800) PATH '$.explicit',
                            t_duration     number        PATH '$.duration_ms',
                            t_album_name   varchar2(800)  PATH '$.album.name',
                            t_album_id     varchar2(800)  PATH '$.album.id',
                            t_release_dt   varchar2(800)  PATH '$.album.release_date',
                            t_artists      varchar2(4000) FORMAT JSON PATH '$.artists',
                            t_album        varchar2(4000) FORMAT JSON PATH '$.album',
                            t_track_json   clob FORMAT JSON PATH '$' error on error
                            )
                        ) j;
                v_offset := v_offset + 50;
                dbms_session.sleep(2);
            end loop;
            --
        end loop;
    end load_pl_tracks;
    --
    procedure load_artists is
        v_ids   varchar2(4000);
        v_cnt   number := 9999;
    begin
        while v_cnt > 0 loop
            select listagg(a_id,',') within group (order by a_id) ids, count(*)
            into v_ids, v_cnt
            from
                (select distinct a_id
                from playlist_tracks t
                    join playlists p on p.pl_id = t.pl_id
                    outer apply json_table(t.t_artists,'$[*]'
                            columns (
                                a_name        varchar2(400) PATH '$.name' null on empty error on error,
                                a_type        varchar2(400) PATH '$.type' null on empty error on error,
                                a_id          varchar2(400) PATH '$.id' null on empty error on error)
                            ) j
                where a_id not in (select art_id from artist)
                fetch first 50 rows only);
            --
            if v_cnt > 0 then
                insert into artist
                select j.art_id, j.art_name, j.art_popularity, j.art_follower_cnt, j.art_genre, j.art_json
                from json_table(
                    fnc_ws_get_json('https://api.spotify.com/v1/artists?ids='||v_ids, 'Bearer '||spotify_api.get_token),
                        '$.artists[*]'
                            columns  (
                                art_id           varchar2(800) PATH '$.id',
                                art_name         varchar2(800) PATH '$.name',
                                art_popularity   NUMBER PATH '$.popularity',
                                art_follower_cnt NUMBER PATH '$.followers.total',
                                art_genre        varchar2(800) FORMAT JSON PATH '$.genres',
                                art_json         varchar2(4000) FORMAT JSON PATH '$')
                    ) j;
                dbms_session.sleep(2);
            end if;
        end loop;
    end load_artists;
    --
    procedure load_track_detail is
        v_ids   varchar2(4000);
        v_cnt   number := 9999;
		cursor c_dtl is
			select j.*
			from json_table(
					fnc_ws_get_json('https://api.spotify.com/v1/audio-features?ids='||v_ids, 'Bearer '||spotify_api.get_token),
					'$.audio_features[*]'
						columns  (
							t_id                  varchar2(200) PATH '$.id',
							acousticness          NUMBER PATH '$.acousticness'    ,
							danceability          NUMBER PATH '$.danceability'    ,
							energy                NUMBER PATH '$.energy'          ,
							instrumentalness      NUMBER PATH '$.instrumentalness',
							pitch_key             NUMBER PATH '$.key'             ,
							liveness              NUMBER PATH '$.liveness'        ,
							loudness              NUMBER PATH '$.loudness'        ,
							major_minor           NUMBER PATH '$.mode'            ,
							speechiness           NUMBER PATH '$.speechiness'     ,
							tempo                 NUMBER PATH '$.tempo'           ,
							time_signature        NUMBER PATH '$.time_signature'  ,
							valence               NUMBER PATH '$.valence'         ,
							features_json         varchar2(4000) FORMAT JSON PATH '$')
				) j;
    begin
		--
		insert into tracks
			( t_name       , t_album_name , t_dur_mins , t_explicit ,
			  artist_names , t_id         , t_album_id , artist_ids )
		select distinct t_name, t_album_name, t_dur_mins, t_explicit,
				json_query(t.t_artists, '$[*].name' WITH WRAPPER) artist_names,
				t_id, t_album_id,
				json_query(t.t_artists, '$[*].id' WITH WRAPPER) artist_ids
		from playlist_tracks t
		where t_id not in (select t_id from tracks);
		--
		commit;
		--
        while v_cnt > 0 loop
            select listagg(t_id,',') within group (order by t_id) ids, count(*)
			into v_ids, v_cnt
            from
                (select t_id
                from tracks t
                where t_features is null
                fetch first 50 rows only);
            --
            if v_cnt > 0 then
				v_cnt := 0;
				for r_dtl in c_dtl loop
					update tracks t
					set t_features       = r_dtl.features_json    ,
						 acousticness     = r_dtl.acousticness     ,
						 danceability     = r_dtl.danceability     ,
						 energy           = r_dtl.energy           ,
						 instrumentalness = r_dtl.instrumentalness ,
						 pitch_key        = r_dtl.pitch_key        ,
						 liveness         = r_dtl.liveness         ,
						 loudness         = r_dtl.loudness         ,
						 major_minor      = r_dtl.major_minor      ,
						 speechiness      = r_dtl.speechiness      ,
						 tempo            = r_dtl.tempo            ,
						 time_signature   = r_dtl.time_signature   ,
						 valence          = r_dtl.valence
					where t.t_id = r_dtl.t_id
					and t_features is null
					and r_dtl.features_json is not null;
					v_cnt := v_cnt + sql%rowcount;
				end loop;
			end if;
			--
			commit;
			--
            dbms_session.sleep(2);
        end loop;
    end load_track_detail;
    --
end spotify_api;
/
